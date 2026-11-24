#!/usr/bin/env python3
import uuid
from client import Client, arguments, check, run, LAB


def main():
    args = arguments('Lab1 edge-case tests; run against a live server')
    check(LAB == 1, 'these tests target lab1')
    c = Client(args.base_url, args.ca_file)

    c.call('GET', '/health/live')
    c.call('GET', '/health/ready')

    # --- malformed JSON ---
    c.call('POST', '/api/v1/users', 400,
           raw=b'{"username":',
           headers={'Content-Type': 'application/json'})
    c.call('POST', '/api/v1/users', 400,
           raw=b'not-json',
           headers={'Content-Type': 'application/json'})
    c.call('POST', '/api/v1/users', 400,
           raw='{"username":"alice","display_name":"А","extra":1}'.encode('utf-8'),
           headers={'Content-Type': 'application/json'})

    # --- username validation / duplicate ---
    u1 = 'u_' + uuid.uuid4().hex[:10]
    body1 = {'username': u1, 'display_name': 'Первый'}
    created = c.call('POST', '/api/v1/users', 201, body=body1, schema='User')['json']
    check(created['username'] == u1, 'username changed')

    c.call('POST', '/api/v1/users', 409, body=body1)
    c.call('POST', '/api/v1/users', 400,
           body={'username': 'Bad Name', 'display_name': 'X'})
    c.call('POST', '/api/v1/users', 400,
           body={'username': 'ab', 'display_name': 'X'})
    c.call('POST', '/api/v1/users', 400,
           body={'username': 'good_user', 'display_name': ''})

    # --- chat creation validations ---
    a = c.user('alice')
    b = c.user('bob')
    e = c.user('eve')

    c.call('POST', '/api/v1/chats', 400, actor=a,
           body={'title': 'no me', 'member_ids': [b['id'], e['id']]})
    c.call('POST', '/api/v1/chats', 400, actor=a,
           body={'title': 'ghost', 'member_ids': [a['id'], str(uuid.uuid4())]})
    c.call('POST', '/api/v1/chats', 400, actor=a,
           body={'title': 'dup', 'member_ids': [a['id'], a['id']]})

    chat = c.call('POST', '/api/v1/chats', 201, actor=a,
                  body={'title': 'Edge', 'member_ids': [a['id'], b['id']]},
                  schema='Chat')['json']

    # --- empty / whitespace-only message ---
    c.call('POST', f"/api/v1/chats/{chat['id']}/messages", 400, actor=a,
           body={'text': ''})
    c.call('POST', f"/api/v1/chats/{chat['id']}/messages", 400, actor=a,
           body={'text': '   \n  '})
    c.call('POST', f"/api/v1/chats/{chat['id']}/messages", 400, actor=a,
           body={'text': 'x' * 4001})

    # --- outsider (eve) forbidden ---
    c.call('GET', f"/api/v1/chats/{chat['id']}", 404, actor=e)
    c.call('GET', f"/api/v1/chats/{chat['id']}/messages", 404, actor=e)
    c.call('POST', f"/api/v1/chats/{chat['id']}/messages", 404, actor=e,
           body={'text': 'intrusion'})
    c.call('GET', f"/api/v1/chats/{chat['id']}", 401)

    # --- unknown UUID ---
    c.call('GET', f"/api/v1/chats/{uuid.uuid4()}", 404, actor=a)
    c.call('GET', f"/api/v1/chats/{uuid.uuid4()}/messages", 404, actor=a)

    # --- multi-page walk (5 pages of 1) ---
    from client import timestamp
    texts = [f'msg-{i}' for i in range(5)]
    created_msgs = [c.message(a, chat, t)['json'] for t in texts]

    # Contract order is (created_at, id) ascending, NOT insertion order.
    def _key(m):
        return (timestamp(m['created_at']), uuid.UUID(m['id']).int)

    expected = [m['text'] for m in sorted(created_msgs, key=_key)]

    seen = []
    cursor = None
    pages = 0
    while True:
        path = f"/api/v1/chats/{chat['id']}/messages?limit=1"
        if cursor:
            from urllib.parse import quote
            path += '&cursor=' + quote(cursor, safe='')
        page = c.call('GET', path, actor=b, schema='MessagePage')['json']
        check(len(page['items']) == 1, 'expected exactly 1 item per page')
        seen.extend(x['text'] for x in page['items'])
        cursor = page['next_cursor']
        pages += 1
        if not cursor:
            break
        check(pages <= 20, 'pagination did not terminate')

    check(seen == expected, f'page walk mismatch: {seen} != {expected}')
    check(pages == 5, f'expected 5 pages, got {pages}')

    # cursor from one chat must not work on another
    other = c.call('POST', '/api/v1/chats', 201, actor=a,
                   body={'title': 'Other', 'member_ids': [a['id'], b['id']]},
                   schema='Chat')['json']
    bad_cursor_path = (f"/api/v1/chats/{other['id']}/messages"
                       f"?limit=1&cursor={cursor or 'x'}")
    c.call('GET', bad_cursor_path, 400, actor=a)

    print('PASS: lab1 extra tests')


if __name__ == '__main__':
    run(main)