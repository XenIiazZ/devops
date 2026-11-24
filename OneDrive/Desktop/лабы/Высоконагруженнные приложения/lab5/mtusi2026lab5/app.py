#!/usr/bin/env python3
import base64, json, os, re, secrets, signal, sys, threading, time, traceback, uuid, hashlib
import bcrypt, psycopg2, psycopg2.pool, redis, boto3, pika
from botocore.exceptions import ClientError
from botocore.config import Config
from PIL import Image
import io
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HOST = os.environ.get("APP_HOST", "0.0.0.0")
PORT = int(os.environ.get("APP_PORT", "8080"))
SESSION_TTL = int(os.environ.get("SESSION_TTL_SECONDS", 3600))
INSTANCE_ID = str(uuid.uuid4())[:8]

s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get("S3_ENDPOINT", "http://rustfs:9000"),
    aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "rustfsadmin"),
    aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "rustfssecret"),
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
S3_BUCKET = os.environ.get("S3_BUCKET", "attachments")

db_pool = psycopg2.pool.ThreadedConnectionPool(
    1, 20,
    host=os.environ.get("DB_HOST", "localhost"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ.get("DB_PASSWORD", "secret"),
    dbname=os.environ.get("DB_NAME", "messenger")
)

redis_client = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=6379,
    decode_responses=True
)

rabbit_conn = None
rabbit_channel = None


def reset_rabbit_channel():
    """Forget a stale RabbitMQ connection so the relay can reconnect."""
    global rabbit_conn, rabbit_channel
    old_conn = rabbit_conn
    rabbit_conn = None
    rabbit_channel = None
    if old_conn is not None:
        try:
            if old_conn.is_open:
                old_conn.close()
        except Exception:
            pass


def get_rabbit_channel():
    """Return a durable, publisher-confirm channel; reconnect after broker restart."""
    global rabbit_conn, rabbit_channel
    if (
        rabbit_conn is None
        or rabbit_conn.is_closed
        or rabbit_channel is None
        or rabbit_channel.is_closed
    ):
        reset_rabbit_channel()
        credentials = pika.PlainCredentials(
            os.environ.get("RABBITMQ_USER", "guest"),
            os.environ.get("RABBITMQ_PASSWORD", "guest"),
        )
        params = pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            credentials=credentials,
            heartbeat=30,
            blocked_connection_timeout=10,
            connection_attempts=1,
            socket_timeout=5,
        )
        conn = pika.BlockingConnection(params)
        try:
            channel = conn.channel()
            channel.exchange_declare(exchange="dlx", exchange_type="direct", durable=True)
            channel.queue_declare(queue="attachment_jobs_dlq", durable=True)
            channel.queue_bind(
                exchange="dlx",
                queue="attachment_jobs_dlq",
                routing_key="attachment_jobs",
            )
            channel.queue_declare(
                queue="attachment_jobs",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "dlx",
                    "x-dead-letter-routing-key": "attachment_jobs",
                },
            )
            # basic_publish now returns a broker confirmation (or raises).
            channel.confirm_delivery()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
        rabbit_conn = conn
        rabbit_channel = channel
    return rabbit_channel


class APIError(Exception):
    def __init__(self, status, code, message):
        self.status = status
        self.code = code
        self.message = message

def format_dt(dt_obj):
    return dt_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def parse_limit(query):
    if "limit" not in query:
        return 50
    values = query["limit"]
    if len(values) != 1:
        raise APIError(400, "invalid_request", "Invalid limit")
    try:
        limit = int(values[0])
    except ValueError:
        raise APIError(400, "invalid_request", "limit must be an integer")
    if limit < 1 or limit > 100:
        raise APIError(400, "invalid_request", "limit must be 1..100")
    return limit

def encode_cursor(resource_type, resource_id, item):
    payload = {"r": resource_type, "id": resource_id, "ts": item["created_at"], "iid": item["id"]}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def decode_cursor(cursor, resource_type, resource_id):
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(cursor + padding)
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("r") != resource_type or payload.get("id") != resource_id:
            raise ValueError()
        return payload["ts"], payload["iid"]
    except Exception:
        raise APIError(400, "invalid_request", "Invalid cursor")

def read_json(handler):
    length = handler.headers.get("Content-Length")
    if not length: raise APIError(400, "invalid_request", "Missing body")
    raw = handler.rfile.read(int(length))
    try: return json.loads(raw.decode("utf-8"))
    except Exception: raise APIError(400, "invalid_request", "Malformed JSON")

def get_user_by_token(handler):
    auth = handler.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        raise APIError(401, "unauthorized", "Missing Bearer token")
    token = auth.split(" ")[1]
    user_id = redis_client.get(f"session:{token}")
    if not user_id:
        raise APIError(401, "unauthorized", "Invalid or expired token")
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, display_name, created_at FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            if not row: raise APIError(401, "unauthorized", "User not found")
            return {
                "id": str(row[0]), "username": row[1], "display_name": row[2],
                "created_at": format_dt(row[3])
            }
    finally:
        db_pool.putconn(conn)

def outbox_relay():
    """Publish committed outbox rows with confirms and always release DB connections.

    Publish-before-commit can produce duplicates after a crash. That is intentional:
    the consumer must be idempotent; it is safer than losing a committed job.
    """
    while True:
        conn = None
        delay = 1
        try:
            conn = db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, payload FROM outbox
                    WHERE published = false
                    ORDER BY created_at
                    LIMIT 10
                    FOR UPDATE SKIP LOCKED
                """)
                rows = cur.fetchall()

                if rows:
                    channel = get_rabbit_channel()
                    for outbox_id, payload in rows:
                        message_id = str(payload.get("job_id", outbox_id))
                        confirmed = channel.basic_publish(
                            exchange="",
                            routing_key="attachment_jobs",
                            body=json.dumps(payload, separators=(",", ":")),
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                                content_type="application/json",
                                message_id=message_id,
                            ),
                            mandatory=True,
                        )
                        if confirmed is False:
                            raise RuntimeError(
                                f"RabbitMQ did not confirm outbox row {outbox_id}"
                            )
                        # Mark only after the broker confirms this message.
                        cur.execute(
                            "UPDATE outbox SET published = true WHERE id = %s",
                            (outbox_id,),
                        )
                conn.commit()
        except Exception:
            delay = 2
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            # A publish/connection failure may leave Pika holding a stale channel.
            reset_rabbit_channel()
            traceback.print_exc()
        finally:
            if conn is not None:
                try:
                    db_pool.putconn(conn, close=bool(conn.closed))
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
        time.sleep(delay)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MessengerLab5/5.0"

    def do_GET(self): self.handle_request("GET")
    def do_POST(self): self.handle_request("POST")
    def do_DELETE(self): self.handle_request("DELETE")

    def handle_request(self, method):
        request_id = str(uuid.uuid4())
        self._status = None
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if method == "GET" and path == "/health/live":
                return self.send_json(200, {"status": "ok"}, request_id)
            if method == "GET" and path == "/health/ready":
                try:
                    conn = db_pool.getconn()
                    with conn.cursor() as cur: cur.execute("SELECT 1")
                    db_pool.putconn(conn)
                    return self.send_json(200, {"status": "ok"}, request_id)
                except Exception:
                    raise APIError(503, "unavailable", "DB not ready")

            if method == "POST" and path == "/api/v1/users": return self.create_user(request_id)
            if method == "POST" and path == "/api/v1/auth/sessions": return self.create_session(request_id)
            if method == "DELETE" and path == "/api/v1/auth/sessions/current": return self.delete_session(request_id)
            if method == "GET" and path == "/api/v1/users/me": return self.get_me(request_id)
            if method == "POST" and path == "/api/v1/chats": return self.create_chat(request_id)
            if method == "GET" and path == "/api/v1/chats": return self.list_chats(request_id, query)

            m = re.fullmatch(r"/api/v1/chats/([^/]+)", path)
            if m and method == "GET": return self.get_chat(request_id, m.group(1))

            m = re.fullmatch(r"/api/v1/chats/([^/]+)/attachments", path)
            if m and method == "POST": return self.create_attachment(request_id, m.group(1), query)

            m = re.fullmatch(r"/api/v1/attachments/([^/]+)", path)
            if m and method == "GET": return self.get_attachment_meta(request_id, m.group(1))

            m = re.fullmatch(r"/api/v1/attachments/([^/]+)/content", path)
            if m and method == "GET": return self.get_attachment_content(request_id, m.group(1))

            m = re.fullmatch(r"/api/v1/chats/([^/]+)/messages", path)
            if m:
                if method == "POST": return self.create_message(request_id, m.group(1))
                if method == "GET": return self.list_messages(request_id, m.group(1), query)

            raise APIError(404, "not_found", "Not found")
        except APIError as exc:
            self.send_error_json(exc.status, exc.code, exc.message, request_id)
        except Exception:
            self.log_error("Unhandled: %s", traceback.format_exc())
            self.send_error_json(500, "internal_error", "Internal error", request_id)

    def create_user(self, request_id):
        body = read_json(self)
        username = body.get("username")
        display_name = body.get("display_name")
        password = body.get("password")
        if not username or not re.fullmatch(r"[a-z0-9_]{3,32}", username):
            raise APIError(400, "invalid_request", "Invalid username")
        if not password or len(password) < 8:
            raise APIError(400, "invalid_request", "Password must be at least 8 chars")

        pw_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        uid = str(uuid.uuid4())

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, username, display_name, password_hash) VALUES (%s, %s, %s, %s) RETURNING created_at",
                    (uid, username, display_name, pw_hash)
                )
                created_at = cur.fetchone()[0]
                conn.commit()
            self.send_json(201, {"id": uid, "username": username, "display_name": display_name, "created_at": format_dt(created_at)}, request_id)
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise APIError(409, "conflict", "Username taken")
        finally:
            db_pool.putconn(conn)

    def create_session(self, request_id):
        auth = self.headers.get("Authorization")
        if not auth or not auth.startswith("Basic "):
            raise APIError(401, "unauthorized", "Missing Basic auth")
        try:
            decoded = base64.b64decode(auth.split(" ")[1]).decode('utf-8')
            username, password = decoded.split(':', 1)
        except Exception:
            raise APIError(401, "unauthorized", "Invalid Basic format")

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, display_name, password_hash, created_at FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
                if not row or not bcrypt.checkpw(password.encode('utf-8'), row[2].encode('utf-8')):
                    raise APIError(401, "unauthorized", "Invalid credentials")

                token = secrets.token_urlsafe(32)
                redis_client.setex(f"session:{token}", SESSION_TTL, row[0])
                expires_at = datetime.now(timezone.utc) + timedelta(seconds=SESSION_TTL)
                
                self.send_json(201, {
                    "token": token, "token_type": "Bearer", "expires_at": format_dt(expires_at),
                    "user": {"id": str(row[0]), "username": username, "display_name": row[1], "created_at": format_dt(row[3])}
                }, request_id)
        finally:
            db_pool.putconn(conn)

    def delete_session(self, request_id):
        auth = self.headers.get("Authorization")
        if auth and auth.startswith("Bearer "):
            token = auth.split(" ")[1]
            if redis_client.get(f"session:{token}"):
                redis_client.delete(f"session:{token}")
                self.send_json(204, None, request_id)
            else:
                raise APIError(401, "unauthorized", "Already logged out")
        else:
            raise APIError(401, "unauthorized", "Missing Bearer")

    def get_me(self, request_id):
        actor = get_user_by_token(self)
        self.send_json(200, actor, request_id)

    def create_chat(self, request_id):
        actor = get_user_by_token(self)
        body = read_json(self)
        title = body.get("title")
        member_ids = body.get("member_ids")
        if not title or not isinstance(member_ids, list):
            raise APIError(400, "invalid_request", "Invalid body")
        
        cid = str(uuid.uuid4())
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO chats (id, title) VALUES (%s, %s) RETURNING created_at", (cid, title))
                created_at = cur.fetchone()[0]
                for uid in member_ids:
                    cur.execute("INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)", (cid, uid))
                conn.commit()
            self.send_json(201, {"id": cid, "title": title, "member_ids": member_ids, "created_at": format_dt(created_at)}, request_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            db_pool.putconn(conn)

    def list_chats(self, request_id, query):
        actor = get_user_by_token(self)
        limit = parse_limit(query)
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.title, c.created_at FROM chats c
                    JOIN chat_members cm ON c.id = cm.chat_id
                    WHERE cm.user_id = %s ORDER BY c.created_at DESC LIMIT %s
                """, (actor["id"], limit + 1))
                rows = cur.fetchall()
                has_more = len(rows) > limit
                rows = rows[:limit]
                
                items = []
                for r in rows:
                    cur.execute("SELECT user_id FROM chat_members WHERE chat_id = %s", (r[0],))
                    members = [str(m[0]) for m in cur.fetchall()]
                    items.append({
                        "id": str(r[0]), "title": r[1], "member_ids": members, "created_at": format_dt(r[2])
                    })
            self.send_json(200, {"items": items, "next_cursor": None}, request_id)
        finally:
            db_pool.putconn(conn)

    def get_chat(self, request_id, chat_id):
        actor = get_user_by_token(self)
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.title, c.created_at FROM chats c
                    JOIN chat_members cm ON c.id = cm.chat_id
                    WHERE c.id = %s AND cm.user_id = %s
                """, (chat_id, actor["id"]))
                row = cur.fetchone()
                if not row: raise APIError(404, "not_found", "Chat not found")
                
                cur.execute("SELECT user_id FROM chat_members WHERE chat_id = %s", (chat_id,))
                members = [str(r[0]) for r in cur.fetchall()]
                
            self.send_json(200, {"id": str(row[0]), "title": row[1], "member_ids": members, "created_at": format_dt(row[2])}, request_id)
        finally:
            db_pool.putconn(conn)

    def create_attachment(self, request_id, chat_id, query):
        actor = get_user_by_token(self)
        filename = query.get("filename", ["upload.bin"])[0]
        if "/" in filename or "\\" in filename or "\x00" in filename:
            raise APIError(400, "invalid_request", "Dangerous filename")

        length = self.headers.get("Content-Length")
        if not length: raise APIError(400, "invalid_request", "Missing body")
        length = int(length)
        if length > 10 * 1024 * 1024:
            raise APIError(413, "payload_too_large", "Max 10MB allowed")

        raw_data = self.rfile.read(length)

        try:
            img = Image.open(io.BytesIO(raw_data))
            if img.format not in ("PNG", "JPEG"):
                raise APIError(415, "unsupported_media_type", "Only PNG/JPEG allowed")
            if img.width * img.height > 40_000_000:
                raise APIError(413, "payload_too_large", "Max 40M pixels allowed")
            mime = "image/png" if img.format == "PNG" else "image/jpeg"
        except APIError:
            raise
        except Exception:
            raise APIError(415, "unsupported_media_type", "Invalid image")

        sha = hashlib.sha256(raw_data).hexdigest()
        aid = str(uuid.uuid4())
        staging_key = f"staging/{aid}"

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
                if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

                try:
                    s3_client.put_object(Bucket=S3_BUCKET, Key=staging_key, Body=raw_data, ContentType=mime)
                except ClientError:
                    raise APIError(500, "internal_error", "S3 staging unavailable")

                cur.execute("""
                    INSERT INTO attachments (id, chat_id, owner_id, filename, content_type, size_bytes, sha256, s3_key, status, staging_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s)
                    RETURNING created_at
                """, (aid, chat_id, actor["id"], filename, mime, len(raw_data), sha, f"{chat_id}/{aid}", staging_key))
                created_at = cur.fetchone()[0]

                job_payload = {
                    "schema_version": 1,
                    "job_id": str(uuid.uuid4()),
                    "type": "attachment.process.v1",
                    "attachment_id": aid,
                    "pipeline_version": 1,
                    "attempt": 1,
                    "created_at": format_dt(datetime.now(timezone.utc))
                }
                cur.execute(
                    "INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, %s, %s)",
                    (aid, "attachment.process.v1", json.dumps(job_payload))
                )
                conn.commit()

            payload = {
                "id": aid, "chat_id": chat_id, "owner_id": actor["id"],
                "filename": filename, "content_type": mime, "size_bytes": len(raw_data),
                "sha256": sha, "status": "queued",
                "variants": [], "error_code": None,
                "created_at": format_dt(created_at),
                "updated_at": format_dt(created_at)
            }
            self.send_json(202, payload, request_id)

        except Exception as e:
            conn.rollback()
            try: s3_client.delete_object(Bucket=S3_BUCKET, Key=staging_key)
            except Exception: pass
            if isinstance(e, APIError): raise
            self.log_error("Attachment upload error: %s", traceback.format_exc())
            raise APIError(500, "internal_error", "Internal error")
        finally:
            db_pool.putconn(conn)

    def get_attachment_meta(self, request_id, att_id):
        actor = get_user_by_token(self)
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.id, a.chat_id, a.owner_id, a.filename, a.content_type, a.size_bytes, a.sha256,
                           a.status, a.error_code, a.created_at, a.updated_at
                    FROM attachments a JOIN chat_members cm ON a.chat_id = cm.chat_id
                    WHERE a.id = %s AND cm.user_id = %s
                """, (att_id, actor["id"]))
                row = cur.fetchone()
                if not row: raise APIError(404, "not_found", "Attachment not found")

                cur.execute("SELECT name, content_type, size_bytes, width, height, sha256 FROM variants WHERE attachment_id = %s", (att_id,))
                variants = []
                for v in cur.fetchall():
                    variants.append({
                        "name": v[0], "content_type": v[1], "size_bytes": v[2],
                        "width": v[3], "height": v[4], "sha256": v[5]
                    })

                self.send_json(200, {
                    "id": str(row[0]), "chat_id": str(row[1]), "owner_id": str(row[2]),
                    "filename": row[3], "content_type": row[4], "size_bytes": row[5],
                    "sha256": row[6], "status": row[7], "error_code": row[8],
                    "variants": variants,
                    "created_at": format_dt(row[9]), "updated_at": format_dt(row[10])
                }, request_id)
        finally:
            db_pool.putconn(conn)

    def get_attachment_content(self, request_id, att_id):
        actor = get_user_by_token(self)
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        variant = query.get("variant", ["original"])[0]

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.s3_key, a.content_type, a.status FROM attachments a
                    JOIN chat_members cm ON a.chat_id = cm.chat_id
                    WHERE a.id = %s AND cm.user_id = %s
                """, (att_id, actor["id"]))
                row = cur.fetchone()
                if not row: raise APIError(404, "not_found", "Attachment not found")
                s3_key, mime, status = row
                if status != 'ready':
                    raise APIError(409, "conflict", "Attachment not ready")

                # Если запрошен вариант (например, thumbnail) — подменяем ключ и MIME
                if variant != "original":
                    cur.execute("""
                        SELECT s3_key, content_type FROM variants
                        WHERE attachment_id = %s AND name = %s
                        ORDER BY pipeline_version DESC LIMIT 1
                    """, (att_id, variant))
                    vrow = cur.fetchone()
                    if not vrow:
                        raise APIError(404, "not_found", f"Variant '{variant}' not found")
                    s3_key, mime = vrow

            try:
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                data = response['Body'].read()
            except ClientError:
                raise APIError(404, "not_found", "File missing from S3")

            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("X-Request-Id", request_id)
            self.send_header("X-Instance-Id", INSTANCE_ID)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        finally:
            db_pool.putconn(conn)

    def create_message(self, request_id, chat_id):
        actor = get_user_by_token(self)
        idem_key = self.headers.get("Idempotency-Key")
        if not idem_key:
            raise APIError(400, "invalid_request", "Idempotency-Key header is required")

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
                if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

                body = read_json(self)
                text = body.get("text", "")
                attachment_ids = body.get("attachment_ids", [])
                
                if not text and not attachment_ids:
                    raise APIError(400, "invalid_request", "Empty text and no attachments")

                body_hash = hashlib.sha256(json.dumps(body, sort_keys=True).encode('utf-8')).hexdigest()
                cur.execute("SELECT message_id, body_hash FROM idempotency_keys WHERE user_id = %s AND idem_key = %s", (actor["id"], idem_key))
                existing = cur.fetchone()

                if existing:
                    if existing[1] != body_hash:
                        raise APIError(409, "conflict", "Idempotency key used with different body")
                    cur.execute("SELECT id, sender_id, text, created_at FROM messages WHERE id = %s", (existing[0],))
                    row = cur.fetchone()
                    return self.send_json(201, {
                        "id": str(row[0]), "chat_id": chat_id, "sender_id": str(row[1]), "text": row[2], 
                        "attachment_ids": attachment_ids, "created_at": format_dt(row[3])
                    }, request_id)

                for aid in attachment_ids:
                    cur.execute("SELECT 1 FROM attachments WHERE id = %s AND chat_id = %s", (aid, chat_id))
                    if not cur.fetchone():
                        raise APIError(404, "not_found", f"Attachment {aid} not found in this chat")

                mid = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO messages (id, chat_id, sender_id, text) VALUES (%s, %s, %s, %s) RETURNING created_at",
                    (mid, chat_id, actor["id"], text)
                )
                created_at = cur.fetchone()[0]
                
                for aid in attachment_ids:
                    cur.execute("INSERT INTO message_attachments (message_id, attachment_id) VALUES (%s, %s)", (mid, aid))
                
                cur.execute(
                    "INSERT INTO idempotency_keys (idem_key, user_id, message_id, body_hash) VALUES (%s, %s, %s, %s)",
                    (idem_key, actor["id"], mid, body_hash)
                )
                conn.commit()
                
            self.send_json(201, {
                "id": mid, "chat_id": chat_id, "sender_id": actor["id"], "text": text, 
                "attachment_ids": attachment_ids, "created_at": format_dt(created_at)
            }, request_id)
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise APIError(409, "conflict", "Concurrent request conflict")
        finally:
            db_pool.putconn(conn)

    def list_messages(self, request_id, chat_id, query):
        actor = get_user_by_token(self)
        limit = parse_limit(query)
        cursor = query.get("cursor", [None])[0]

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
                if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

                if cursor:
                    ts, iid = decode_cursor(cursor, "messages", chat_id)
                    cur.execute("""
                        SELECT id, sender_id, text, created_at FROM messages 
                        WHERE chat_id = %s AND (created_at, id) > (%s, %s)
                        ORDER BY created_at, id LIMIT %s
                    """, (chat_id, ts, iid, limit + 1))
                else:
                    cur.execute("""
                        SELECT id, sender_id, text, created_at FROM messages 
                        WHERE chat_id = %s ORDER BY created_at, id LIMIT %s
                    """, (chat_id, limit + 1))
                
                rows = cur.fetchall()
                has_more = len(rows) > limit
                rows = rows[:limit]
                
                items = []
                for r in rows:
                    mid = r[0]
                    cur.execute("SELECT attachment_id FROM message_attachments WHERE message_id = %s", (mid,))
                    aids = [str(a[0]) for a in cur.fetchall()]
                    
                    items.append({
                        "id": str(r[0]), "chat_id": chat_id, "sender_id": str(r[1]), 
                        "text": r[2], "attachment_ids": aids, "created_at": format_dt(r[3])
                    })
                
                next_cursor = encode_cursor("messages", chat_id, items[-1]) if has_more and items else None
                
            self.send_json(200, {"items": items, "next_cursor": next_cursor}, request_id)
        finally:
            db_pool.putconn(conn)

    def send_json(self, status, payload, request_id):
        self._status = status
        if status == 204:
            self.send_response(204)
            self.send_header("X-Request-Id", request_id)
            self.send_header("X-Instance-Id", INSTANCE_ID)
            self.end_headers()
            return
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Request-Id", request_id)
        self.send_header("X-Instance-Id", INSTANCE_ID)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, status, code, message, request_id):
        self._status = status
        body = json.dumps({"error": {"code": code, "message": message, "request_id": request_id}}, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if status == 401:
            parsed_path = urlparse(self.path).path
            if parsed_path == "/api/v1/auth/sessions":
                self.send_header("WWW-Authenticate", 'Basic realm="messenger", charset="UTF-8"')
            else:
                self.send_header("WWW-Authenticate", 'Bearer realm="messenger"')

        self.send_header("Content-Type", "application/json")
        self.send_header("X-Request-Id", request_id)
        self.send_header("X-Instance-Id", INSTANCE_ID)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def main():
    threading.Thread(target=outbox_relay, daemon=True).start()
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    def shutdown(signum, frame):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    print(f"Listening on {HOST}:{PORT}", file=sys.stderr)
    try:
        server.serve_forever()
    finally:
        server.server_close()

if __name__ == "__main__":
    main()



# #!/usr/bin/env python3
# import base64, json, os, re, secrets, signal, sys, threading, time, traceback, uuid, hashlib
# import bcrypt, psycopg2, psycopg2.pool, redis, boto3, pika
# from botocore.exceptions import ClientError
# from botocore.config import Config
# from PIL import Image
# import io
# from datetime import datetime, timezone, timedelta
# from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
# from urllib.parse import urlparse, parse_qs

# HOST = os.environ.get("APP_HOST", "0.0.0.0")
# PORT = int(os.environ.get("APP_PORT", "8080"))
# SESSION_TTL = int(os.environ.get("SESSION_TTL_SECONDS", 3600))
# INSTANCE_ID = str(uuid.uuid4())[:8]

# s3_client = boto3.client(
#     's3',
#     endpoint_url=os.environ.get("S3_ENDPOINT", "http://rustfs:9000"),
#     aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "rustfsadmin"),
#     aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "rustfssecret"),
#     config=Config(signature_version='s3v4'),
#     region_name='us-east-1'
# )
# S3_BUCKET = os.environ.get("S3_BUCKET", "attachments")

# db_pool = psycopg2.pool.ThreadedConnectionPool(
#     1, 20,
#     host=os.environ.get("DB_HOST", "localhost"),
#     user=os.environ.get("DB_USER", "postgres"),
#     password=os.environ.get("DB_PASSWORD", "secret"),
#     dbname=os.environ.get("DB_NAME", "messenger")
# )

# redis_client = redis.Redis(
#     host=os.environ.get("REDIS_HOST", "localhost"),
#     port=6379,
#     decode_responses=True
# )

# rabbit_conn = None
# rabbit_channel = None
# def get_rabbit_channel():
#     global rabbit_conn, rabbit_channel
#     if rabbit_channel is None or rabbit_channel.is_closed:
#         rabbit_conn = pika.BlockingConnection(
#             pika.ConnectionParameters(host=os.environ.get("RABBITMQ_HOST", "rabbitmq"))
#         )
#         rabbit_channel = rabbit_conn.channel()
#         # ОБЪЯВЛЯЕМ DLX и очередь с аргументами, как в worker
#         rabbit_channel.exchange_declare(exchange='dlx', exchange_type='direct', durable=True)
#         rabbit_channel.queue_declare(queue='attachment_jobs_dlq', durable=True)
#         rabbit_channel.queue_bind(exchange='dlx', queue='attachment_jobs_dlq', routing_key='attachment_jobs')
#         rabbit_channel.queue_declare(queue='attachment_jobs', durable=True, arguments={
#             'x-dead-letter-exchange': 'dlx',
#             'x-dead-letter-routing-key': 'attachment_jobs'
#         })
#     return rabbit_channel

# class APIError(Exception):
#     def __init__(self, status, code, message):
#         self.status = status
#         self.code = code
#         self.message = message

# def format_dt(dt_obj):
#     return dt_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# def parse_limit(query):
#     if "limit" not in query:
#         return 50
#     values = query["limit"]
#     if len(values) != 1:
#         raise APIError(400, "invalid_request", "Invalid limit")
#     try:
#         limit = int(values[0])
#     except ValueError:
#         raise APIError(400, "invalid_request", "limit must be an integer")
#     if limit < 1 or limit > 100:
#         raise APIError(400, "invalid_request", "limit must be 1..100")
#     return limit

# def encode_cursor(resource_type, resource_id, item):
#     payload = {"r": resource_type, "id": resource_id, "ts": item["created_at"], "iid": item["id"]}
#     raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
#     return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

# def decode_cursor(cursor, resource_type, resource_id):
#     try:
#         padding = "=" * (-len(cursor) % 4)
#         raw = base64.urlsafe_b64decode(cursor + padding)
#         payload = json.loads(raw.decode("utf-8"))
#         if payload.get("r") != resource_type or payload.get("id") != resource_id:
#             raise ValueError()
#         return payload["ts"], payload["iid"]
#     except Exception:
#         raise APIError(400, "invalid_request", "Invalid cursor")

# def read_json(handler):
#     length = handler.headers.get("Content-Length")
#     if not length: raise APIError(400, "invalid_request", "Missing body")
#     raw = handler.rfile.read(int(length))
#     try: return json.loads(raw.decode("utf-8"))
#     except Exception: raise APIError(400, "invalid_request", "Malformed JSON")

# def get_user_by_token(handler):
#     auth = handler.headers.get("Authorization")
#     if not auth or not auth.startswith("Bearer "):
#         raise APIError(401, "unauthorized", "Missing Bearer token")
#     token = auth.split(" ")[1]
#     user_id = redis_client.get(f"session:{token}")
#     if not user_id:
#         raise APIError(401, "unauthorized", "Invalid or expired token")
#     conn = db_pool.getconn()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("SELECT id, username, display_name, created_at FROM users WHERE id = %s", (user_id,))
#             row = cur.fetchone()
#             if not row: raise APIError(401, "unauthorized", "User not found")
#             return {
#                 "id": str(row[0]), "username": row[1], "display_name": row[2],
#                 "created_at": format_dt(row[3])
#             }
#     finally:
#         db_pool.putconn(conn)

# def outbox_relay():
#     while True:
#         try:
#             conn = db_pool.getconn()
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT id, payload FROM outbox
#                     WHERE published = false
#                     ORDER BY created_at
#                     LIMIT 10
#                     FOR UPDATE SKIP LOCKED
#                 """)
#                 rows = cur.fetchall()
#                 if not rows:
#                     conn.commit()
#                     db_pool.putconn(conn)
#                     time.sleep(1)
#                     continue
#                 ch = get_rabbit_channel()
#                 for row in rows:
#                     outbox_id, payload = row
#                     try:
#                         ch.basic_publish(
#                             exchange='',
#                             routing_key='attachment_jobs',
#                             body=json.dumps(payload),
#                             properties=pika.BasicProperties(delivery_mode=2)
#                         )
#                         cur.execute("UPDATE outbox SET published = true WHERE id = %s", (outbox_id,))
#                     except Exception as e:
#                         print(f"Relay publish failed: {e}", file=sys.stderr)
#                 conn.commit()
#             db_pool.putconn(conn)
#         except Exception:
#             traceback.print_exc()
#             time.sleep(2)

# class Handler(BaseHTTPRequestHandler):
#     protocol_version = "HTTP/1.1"
#     server_version = "MessengerLab5/5.0"

#     def do_GET(self): self.handle_request("GET")
#     def do_POST(self): self.handle_request("POST")
#     def do_DELETE(self): self.handle_request("DELETE")

#     def handle_request(self, method):
#         request_id = str(uuid.uuid4())
#         self._status = None
#         try:
#             parsed = urlparse(self.path)
#             path = parsed.path
#             query = parse_qs(parsed.query, keep_blank_values=False)

#             if method == "GET" and path == "/health/live":
#                 return self.send_json(200, {"status": "ok"}, request_id)
#             if method == "GET" and path == "/health/ready":
#                 try:
#                     conn = db_pool.getconn()
#                     with conn.cursor() as cur: cur.execute("SELECT 1")
#                     db_pool.putconn(conn)
#                     return self.send_json(200, {"status": "ok"}, request_id)
#                 except Exception:
#                     raise APIError(503, "unavailable", "DB not ready")

#             if method == "POST" and path == "/api/v1/users": return self.create_user(request_id)
#             if method == "POST" and path == "/api/v1/auth/sessions": return self.create_session(request_id)
#             if method == "DELETE" and path == "/api/v1/auth/sessions/current": return self.delete_session(request_id)
#             if method == "GET" and path == "/api/v1/users/me": return self.get_me(request_id)
#             if method == "POST" and path == "/api/v1/chats": return self.create_chat(request_id)
#             if method == "GET" and path == "/api/v1/chats": return self.list_chats(request_id, query)

#             m = re.fullmatch(r"/api/v1/chats/([^/]+)", path)
#             if m and method == "GET": return self.get_chat(request_id, m.group(1))

#             m = re.fullmatch(r"/api/v1/chats/([^/]+)/attachments", path)
#             if m and method == "POST": return self.create_attachment(request_id, m.group(1), query)

#             m = re.fullmatch(r"/api/v1/attachments/([^/]+)", path)
#             if m and method == "GET": return self.get_attachment_meta(request_id, m.group(1))

#             m = re.fullmatch(r"/api/v1/attachments/([^/]+)/content", path)
#             if m and method == "GET": return self.get_attachment_content(request_id, m.group(1))

#             m = re.fullmatch(r"/api/v1/chats/([^/]+)/messages", path)
#             if m:
#                 if method == "POST": return self.create_message(request_id, m.group(1))
#                 if method == "GET": return self.list_messages(request_id, m.group(1), query)

#             raise APIError(404, "not_found", "Not found")
#         except APIError as exc:
#             self.send_error_json(exc.status, exc.code, exc.message, request_id)
#         except Exception:
#             self.log_error("Unhandled: %s", traceback.format_exc())
#             self.send_error_json(500, "internal_error", "Internal error", request_id)

#     def create_user(self, request_id):
#         body = read_json(self)
#         username = body.get("username")
#         display_name = body.get("display_name")
#         password = body.get("password")
#         if not username or not re.fullmatch(r"[a-z0-9_]{3,32}", username):
#             raise APIError(400, "invalid_request", "Invalid username")
#         if not password or len(password) < 8:
#             raise APIError(400, "invalid_request", "Password must be at least 8 chars")

#         pw_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
#         uid = str(uuid.uuid4())

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute(
#                     "INSERT INTO users (id, username, display_name, password_hash) VALUES (%s, %s, %s, %s) RETURNING created_at",
#                     (uid, username, display_name, pw_hash)
#                 )
#                 created_at = cur.fetchone()[0]
#                 conn.commit()
#             self.send_json(201, {"id": uid, "username": username, "display_name": display_name, "created_at": format_dt(created_at)}, request_id)
#         except psycopg2.errors.UniqueViolation:
#             conn.rollback()
#             raise APIError(409, "conflict", "Username taken")
#         finally:
#             db_pool.putconn(conn)

#     def create_session(self, request_id):
#         auth = self.headers.get("Authorization")
#         if not auth or not auth.startswith("Basic "):
#             raise APIError(401, "unauthorized", "Missing Basic auth")
#         try:
#             decoded = base64.b64decode(auth.split(" ")[1]).decode('utf-8')
#             username, password = decoded.split(':', 1)
#         except Exception:
#             raise APIError(401, "unauthorized", "Invalid Basic format")

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("SELECT id, display_name, password_hash, created_at FROM users WHERE username = %s", (username,))
#                 row = cur.fetchone()
#                 if not row or not bcrypt.checkpw(password.encode('utf-8'), row[2].encode('utf-8')):
#                     raise APIError(401, "unauthorized", "Invalid credentials")

#                 token = secrets.token_urlsafe(32)
#                 redis_client.setex(f"session:{token}", SESSION_TTL, row[0])
#                 expires_at = datetime.now(timezone.utc) + timedelta(seconds=SESSION_TTL)
                
#                 self.send_json(201, {
#                     "token": token, "token_type": "Bearer", "expires_at": format_dt(expires_at),
#                     "user": {"id": str(row[0]), "username": username, "display_name": row[1], "created_at": format_dt(row[3])}
#                 }, request_id)
#         finally:
#             db_pool.putconn(conn)

#     def delete_session(self, request_id):
#         auth = self.headers.get("Authorization")
#         if auth and auth.startswith("Bearer "):
#             token = auth.split(" ")[1]
#             if redis_client.get(f"session:{token}"):
#                 redis_client.delete(f"session:{token}")
#                 self.send_json(204, None, request_id)
#             else:
#                 raise APIError(401, "unauthorized", "Already logged out")
#         else:
#             raise APIError(401, "unauthorized", "Missing Bearer")

#     def get_me(self, request_id):
#         actor = get_user_by_token(self)
#         self.send_json(200, actor, request_id)

#     def create_chat(self, request_id):
#         actor = get_user_by_token(self)
#         body = read_json(self)
#         title = body.get("title")
#         member_ids = body.get("member_ids")
#         if not title or not isinstance(member_ids, list):
#             raise APIError(400, "invalid_request", "Invalid body")
        
#         cid = str(uuid.uuid4())
#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("INSERT INTO chats (id, title) VALUES (%s, %s) RETURNING created_at", (cid, title))
#                 created_at = cur.fetchone()[0]
#                 for uid in member_ids:
#                     cur.execute("INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)", (cid, uid))
#                 conn.commit()
#             self.send_json(201, {"id": cid, "title": title, "member_ids": member_ids, "created_at": format_dt(created_at)}, request_id)
#         except Exception:
#             conn.rollback()
#             raise
#         finally:
#             db_pool.putconn(conn)

#     def list_chats(self, request_id, query):
#         actor = get_user_by_token(self)
#         limit = parse_limit(query)
#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT c.id, c.title, c.created_at FROM chats c
#                     JOIN chat_members cm ON c.id = cm.chat_id
#                     WHERE cm.user_id = %s ORDER BY c.created_at DESC LIMIT %s
#                 """, (actor["id"], limit + 1))
#                 rows = cur.fetchall()
#                 has_more = len(rows) > limit
#                 rows = rows[:limit]
                
#                 items = []
#                 for r in rows:
#                     cur.execute("SELECT user_id FROM chat_members WHERE chat_id = %s", (r[0],))
#                     members = [str(m[0]) for m in cur.fetchall()]
#                     items.append({
#                         "id": str(r[0]), "title": r[1], "member_ids": members, "created_at": format_dt(r[2])
#                     })
#             self.send_json(200, {"items": items, "next_cursor": None}, request_id)
#         finally:
#             db_pool.putconn(conn)

#     def get_chat(self, request_id, chat_id):
#         actor = get_user_by_token(self)
#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT c.id, c.title, c.created_at FROM chats c
#                     JOIN chat_members cm ON c.id = cm.chat_id
#                     WHERE c.id = %s AND cm.user_id = %s
#                 """, (chat_id, actor["id"]))
#                 row = cur.fetchone()
#                 if not row: raise APIError(404, "not_found", "Chat not found")
                
#                 cur.execute("SELECT user_id FROM chat_members WHERE chat_id = %s", (chat_id,))
#                 members = [str(r[0]) for r in cur.fetchall()]
                
#             self.send_json(200, {"id": str(row[0]), "title": row[1], "member_ids": members, "created_at": format_dt(row[2])}, request_id)
#         finally:
#             db_pool.putconn(conn)

#     def create_attachment(self, request_id, chat_id, query):
#         actor = get_user_by_token(self)
#         filename = query.get("filename", ["upload.bin"])[0]
#         if "/" in filename or "\\" in filename or "\x00" in filename:
#             raise APIError(400, "invalid_request", "Dangerous filename")

#         length = self.headers.get("Content-Length")
#         if not length: raise APIError(400, "invalid_request", "Missing body")
#         length = int(length)
#         if length > 10 * 1024 * 1024:
#             raise APIError(413, "payload_too_large", "Max 10MB allowed")

#         raw_data = self.rfile.read(length)

#         try:
#             img = Image.open(io.BytesIO(raw_data))
#             if img.format not in ("PNG", "JPEG"):
#                 raise APIError(415, "unsupported_media_type", "Only PNG/JPEG allowed")
#             if img.width * img.height > 40_000_000:
#                 raise APIError(413, "payload_too_large", "Max 40M pixels allowed")
#             mime = "image/png" if img.format == "PNG" else "image/jpeg"
#         except APIError:
#             raise
#         except Exception:
#             raise APIError(415, "unsupported_media_type", "Invalid image")

#         sha = hashlib.sha256(raw_data).hexdigest()
#         aid = str(uuid.uuid4())
#         staging_key = f"staging/{aid}"

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
#                 if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

#                 try:
#                     s3_client.put_object(Bucket=S3_BUCKET, Key=staging_key, Body=raw_data, ContentType=mime)
#                 except ClientError:
#                     raise APIError(500, "internal_error", "S3 staging unavailable")

#                 cur.execute("""
#                     INSERT INTO attachments (id, chat_id, owner_id, filename, content_type, size_bytes, sha256, s3_key, status, staging_key)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'queued', %s)
#                     RETURNING created_at
#                 """, (aid, chat_id, actor["id"], filename, mime, len(raw_data), sha, f"{chat_id}/{aid}", staging_key))
#                 created_at = cur.fetchone()[0]

#                 job_payload = {
#                     "schema_version": 1,
#                     "job_id": str(uuid.uuid4()),
#                     "type": "attachment.process.v1",
#                     "attachment_id": aid,
#                     "pipeline_version": 1,
#                     "attempt": 1,
#                     "created_at": format_dt(datetime.now(timezone.utc))
#                 }
#                 cur.execute(
#                     "INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (%s, %s, %s)",
#                     (aid, "attachment.process.v1", json.dumps(job_payload))
#                 )
#                 conn.commit()

#             payload = {
#                 "id": aid, "chat_id": chat_id, "owner_id": actor["id"],
#                 "filename": filename, "content_type": mime, "size_bytes": len(raw_data),
#                 "sha256": sha, "status": "queued",
#                 "variants": [], "error_code": None,
#                 "created_at": format_dt(created_at),
#                 "updated_at": format_dt(created_at)
#             }
#             self.send_json(202, payload, request_id)

#         except Exception as e:
#             conn.rollback()
#             try: s3_client.delete_object(Bucket=S3_BUCKET, Key=staging_key)
#             except Exception: pass
#             if isinstance(e, APIError): raise
#             self.log_error("Attachment upload error: %s", traceback.format_exc())
#             raise APIError(500, "internal_error", "Internal error")
#         finally:
#             db_pool.putconn(conn)

#     def get_attachment_meta(self, request_id, att_id):
#         actor = get_user_by_token(self)
#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT a.id, a.chat_id, a.owner_id, a.filename, a.content_type, a.size_bytes, a.sha256,
#                            a.status, a.error_code, a.created_at, a.updated_at
#                     FROM attachments a JOIN chat_members cm ON a.chat_id = cm.chat_id
#                     WHERE a.id = %s AND cm.user_id = %s
#                 """, (att_id, actor["id"]))
#                 row = cur.fetchone()
#                 if not row: raise APIError(404, "not_found", "Attachment not found")

#                 cur.execute("SELECT name, content_type, size_bytes, width, height, sha256 FROM variants WHERE attachment_id = %s", (att_id,))
#                 variants = []
#                 for v in cur.fetchall():
#                     variants.append({
#                         "name": v[0], "content_type": v[1], "size_bytes": v[2],
#                         "width": v[3], "height": v[4], "sha256": v[5]
#                     })

#                 self.send_json(200, {
#                     "id": str(row[0]), "chat_id": str(row[1]), "owner_id": str(row[2]),
#                     "filename": row[3], "content_type": row[4], "size_bytes": row[5],
#                     "sha256": row[6], "status": row[7], "error_code": row[8],
#                     "variants": variants,
#                     "created_at": format_dt(row[9]), "updated_at": format_dt(row[10])
#                 }, request_id)
#         finally:
#             db_pool.putconn(conn)

#     def get_attachment_content(self, request_id, att_id):
#         actor = get_user_by_token(self)
#         parsed = urlparse(self.path)
#         query = parse_qs(parsed.query)
#         variant = query.get("variant", ["original"])[0]

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("""
#                     SELECT a.s3_key, a.content_type, a.status FROM attachments a
#                     JOIN chat_members cm ON a.chat_id = cm.chat_id
#                     WHERE a.id = %s AND cm.user_id = %s
#                 """, (att_id, actor["id"]))
#                 row = cur.fetchone()
#                 if not row: raise APIError(404, "not_found", "Attachment not found")
#                 s3_key, mime, status = row
#                 if status != 'ready':
#                     raise APIError(409, "conflict", "Attachment not ready")

#                 # Если запрошен вариант (например, thumbnail) — подменяем ключ и MIME
#                 if variant != "original":
#                     cur.execute("""
#                         SELECT s3_key, content_type FROM variants
#                         WHERE attachment_id = %s AND name = %s
#                         ORDER BY pipeline_version DESC LIMIT 1
#                     """, (att_id, variant))
#                     vrow = cur.fetchone()
#                     if not vrow:
#                         raise APIError(404, "not_found", f"Variant '{variant}' not found")
#                     s3_key, mime = vrow

#             try:
#                 response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
#                 data = response['Body'].read()
#             except ClientError:
#                 raise APIError(404, "not_found", "File missing from S3")

#             self.send_response(200)
#             self.send_header("Content-Type", mime)
#             self.send_header("X-Request-Id", request_id)
#             self.send_header("X-Instance-Id", INSTANCE_ID)
#             self.send_header("Content-Length", str(len(data)))
#             self.end_headers()
#             self.wfile.write(data)
#         finally:
#             db_pool.putconn(conn)

#     def create_message(self, request_id, chat_id):
#         actor = get_user_by_token(self)
#         idem_key = self.headers.get("Idempotency-Key")
#         if not idem_key:
#             raise APIError(400, "invalid_request", "Idempotency-Key header is required")

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
#                 if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

#                 body = read_json(self)
#                 text = body.get("text", "")
#                 attachment_ids = body.get("attachment_ids", [])
                
#                 if not text and not attachment_ids:
#                     raise APIError(400, "invalid_request", "Empty text and no attachments")

#                 body_hash = hashlib.sha256(json.dumps(body, sort_keys=True).encode('utf-8')).hexdigest()
#                 cur.execute("SELECT message_id, body_hash FROM idempotency_keys WHERE user_id = %s AND idem_key = %s", (actor["id"], idem_key))
#                 existing = cur.fetchone()

#                 if existing:
#                     if existing[1] != body_hash:
#                         raise APIError(409, "conflict", "Idempotency key used with different body")
#                     cur.execute("SELECT id, sender_id, text, created_at FROM messages WHERE id = %s", (existing[0],))
#                     row = cur.fetchone()
#                     return self.send_json(201, {
#                         "id": str(row[0]), "chat_id": chat_id, "sender_id": str(row[1]), "text": row[2], 
#                         "attachment_ids": attachment_ids, "created_at": format_dt(row[3])
#                     }, request_id)

#                 for aid in attachment_ids:
#                     cur.execute("SELECT 1 FROM attachments WHERE id = %s AND chat_id = %s", (aid, chat_id))
#                     if not cur.fetchone():
#                         raise APIError(404, "not_found", f"Attachment {aid} not found in this chat")

#                 mid = str(uuid.uuid4())
#                 cur.execute(
#                     "INSERT INTO messages (id, chat_id, sender_id, text) VALUES (%s, %s, %s, %s) RETURNING created_at",
#                     (mid, chat_id, actor["id"], text)
#                 )
#                 created_at = cur.fetchone()[0]
                
#                 for aid in attachment_ids:
#                     cur.execute("INSERT INTO message_attachments (message_id, attachment_id) VALUES (%s, %s)", (mid, aid))
                
#                 cur.execute(
#                     "INSERT INTO idempotency_keys (idem_key, user_id, message_id, body_hash) VALUES (%s, %s, %s, %s)",
#                     (idem_key, actor["id"], mid, body_hash)
#                 )
#                 conn.commit()
                
#             self.send_json(201, {
#                 "id": mid, "chat_id": chat_id, "sender_id": actor["id"], "text": text, 
#                 "attachment_ids": attachment_ids, "created_at": format_dt(created_at)
#             }, request_id)
#         except psycopg2.errors.UniqueViolation:
#             conn.rollback()
#             raise APIError(409, "conflict", "Concurrent request conflict")
#         finally:
#             db_pool.putconn(conn)

#     def list_messages(self, request_id, chat_id, query):
#         actor = get_user_by_token(self)
#         limit = parse_limit(query)
#         cursor = query.get("cursor", [None])[0]

#         conn = db_pool.getconn()
#         try:
#             with conn.cursor() as cur:
#                 cur.execute("SELECT 1 FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, actor["id"]))
#                 if not cur.fetchone(): raise APIError(404, "not_found", "Chat not found")

#                 if cursor:
#                     ts, iid = decode_cursor(cursor, "messages", chat_id)
#                     cur.execute("""
#                         SELECT id, sender_id, text, created_at FROM messages 
#                         WHERE chat_id = %s AND (created_at, id) > (%s, %s)
#                         ORDER BY created_at, id LIMIT %s
#                     """, (chat_id, ts, iid, limit + 1))
#                 else:
#                     cur.execute("""
#                         SELECT id, sender_id, text, created_at FROM messages 
#                         WHERE chat_id = %s ORDER BY created_at, id LIMIT %s
#                     """, (chat_id, limit + 1))
                
#                 rows = cur.fetchall()
#                 has_more = len(rows) > limit
#                 rows = rows[:limit]
                
#                 items = []
#                 for r in rows:
#                     mid = r[0]
#                     cur.execute("SELECT attachment_id FROM message_attachments WHERE message_id = %s", (mid,))
#                     aids = [str(a[0]) for a in cur.fetchall()]
                    
#                     items.append({
#                         "id": str(r[0]), "chat_id": chat_id, "sender_id": str(r[1]), 
#                         "text": r[2], "attachment_ids": aids, "created_at": format_dt(r[3])
#                     })
                
#                 next_cursor = encode_cursor("messages", chat_id, items[-1]) if has_more and items else None
                
#             self.send_json(200, {"items": items, "next_cursor": next_cursor}, request_id)
#         finally:
#             db_pool.putconn(conn)

#     def send_json(self, status, payload, request_id):
#         self._status = status
#         if status == 204:
#             self.send_response(204)
#             self.send_header("X-Request-Id", request_id)
#             self.send_header("X-Instance-Id", INSTANCE_ID)
#             self.end_headers()
#             return
#         body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
#         self.send_response(status)
#         self.send_header("Content-Type", "application/json")
#         self.send_header("X-Request-Id", request_id)
#         self.send_header("X-Instance-Id", INSTANCE_ID)
#         self.send_header("Content-Length", str(len(body)))
#         self.end_headers()
#         self.wfile.write(body)

#     def send_error_json(self, status, code, message, request_id):
#         self._status = status
#         body = json.dumps({"error": {"code": code, "message": message, "request_id": request_id}}, ensure_ascii=False).encode("utf-8")
#         self.send_response(status)
#         if status == 401:
#             parsed_path = urlparse(self.path).path
#             if parsed_path == "/api/v1/auth/sessions":
#                 self.send_header("WWW-Authenticate", 'Basic realm="messenger", charset="UTF-8"')
#             else:
#                 self.send_header("WWW-Authenticate", 'Bearer realm="messenger"')

#         self.send_header("Content-Type", "application/json")
#         self.send_header("X-Request-Id", request_id)
#         self.send_header("X-Instance-Id", INSTANCE_ID)
#         self.send_header("Content-Length", str(len(body)))
#         self.end_headers()
#         self.wfile.write(body)

# def main():
#     threading.Thread(target=outbox_relay, daemon=True).start()
#     ThreadingHTTPServer.daemon_threads = True
#     ThreadingHTTPServer.allow_reuse_address = True
#     server = ThreadingHTTPServer((HOST, PORT), Handler)
#     def shutdown(signum, frame):
#         threading.Thread(target=server.shutdown, daemon=True).start()
#     signal.signal(signal.SIGTERM, shutdown)
#     signal.signal(signal.SIGINT, shutdown)
#     print(f"Listening on {HOST}:{PORT}", file=sys.stderr)
#     try:
#         server.serve_forever()
#     finally:
#         server.server_close()

# if __name__ == "__main__":
#     main()