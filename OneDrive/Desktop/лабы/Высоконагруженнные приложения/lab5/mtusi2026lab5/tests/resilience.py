#!/usr/bin/env python3
"""Explicit operator-authored hook; tokens remain in memory, volumes are never removed by this test."""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from urllib.parse import quote
from client import Client, Failure, LAB, arguments, check, run


def history(c, actor, chat):
    path = '/api/v1/chats/'+chat['id']+'/messages?limit=100'
    result = []
    seen = set()
    for _ in range(1000):
        page = c.call('GET',path,actor=actor,schema='MessagePage')['json']
        result.extend(page['items'])
        cursor = page['next_cursor']
        if cursor is None:
            return result
        check(cursor not in seen,'cursor cycle')
        seen.add(cursor)
        path = '/api/v1/chats/'+chat['id']+'/messages?limit=100&cursor='+quote(cursor,safe='')
    raise Failure('history pagination exceeded safety bound')


def main():
    def configure(p):
        p.add_argument('--mode', choices=['persistence','failover'], required=True)
        p.add_argument('--action-script',required=True,help='Executable script authored by student; restarts stack or stops ONE API replica')
    args = arguments('Explicit restart/failover scenario; modifies only synthetic API data',configure)
    check(LAB>=2,'persistence requires lab2+')
    check(args.mode!='failover' or LAB>=3,'failover requires lab3+')
    script = Path(args.action_script).expanduser().resolve()
    check(script.is_file() and os.access(script,os.X_OK),'action script must exist and be executable (chmod +x)')
    check('.example.' not in script.name,'copy and implement the example hook first')
    c = Client(args.base_url,args.ca_file,timeout=2)
    alice,bob,eve,chat = c.bootstrap()
    sent = c.message(alice,chat,'Сохранить при отказе')
    original = sent['json']
    target = sent['headers'].get('X-Instance-Id','')
    attachment = content = None
    if LAB>=4:
        attachment,content = c.attachment(alice,chat)
        if LAB>=5:
            attachment = c.wait_ready(alice,attachment)
    env = dict(os.environ)
    env['TARGET_INSTANCE_ID'] = target
    observed = set()
    if args.mode=='failover':
        for _ in range(40):
            observed.add(c.call('GET','/api/v1/users/me',actor=alice,schema='User')['headers']['X-Instance-Id'])
        check(len(observed)>=2,'less than two API instances observed before failure; verify LB/session affinity')
    print('Executing explicit '+args.mode+' hook; hook output suppressed to avoid leaking secrets.')
    # No shell interpolation and no Docker commands in this test.
    proc = subprocess.Popen([str(script)],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    started = time.monotonic()
    successes,failures,after_instances = [],0,[]
    try:
        if args.mode=='persistence':
            try:
                code = proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                raise Failure('restart hook exceeded 90 seconds') from None
            check(code==0,'restart hook failed (inspect your script locally)')
            deadline = time.monotonic()+90
            while True:
                try:
                    c.call('GET','/health/ready',schema='Health')
                    c.call('GET','/api/v1/users/me',actor=alice,schema='User')
                    break
                except Failure:
                    check(time.monotonic()<deadline,'stack/session did not recover within 90 seconds')
                    time.sleep(0.5)
        else:
            first_recovery = None
            key = uuid.uuid4().hex
            text = 'Запись во время отказа ' + key
            while time.monotonic()-started < 15:
                code = proc.poll()
                check(code is None or code==0,'stop-one hook failed (inspect your script locally)')
                check(code is not None or time.monotonic()-started<10,'stop-one hook must return within 10 seconds')
                try:
                    response = c.message(alice,chat,text,key=key)
                    msg = response['json']
                    if code==0:
                        if first_recovery is None:
                            first_recovery = time.monotonic()-started
                        after_instances.append(response['headers']['X-Instance-Id'])
                    successes.append(msg)
                    key = uuid.uuid4().hex
                    text = 'Запись во время отказа ' + key
                except Failure:
                    failures += 1
                    # Keep SAME key/body after an uncertain POST result.
                time.sleep(0.1)
            check(proc.poll()==0,'stop-one hook did not finish successfully')
            check(first_recovery is not None and first_recovery<=10,'writes did not recover within 10 seconds from hook start')
            check(len(after_instances)>=5,'too few successful writes after stop')
            check(target not in after_instances[-5:],'target replica still serves requests; hook must leave it stopped')
            print(json.dumps({'observed_instances_before':len(observed),'successful_writes':len(successes),'failed_attempts':failures,'first_recovery_seconds':round(first_recovery,3)},ensure_ascii=False))
        # Existing token, no re-login. Read all confirmations and reject duplicates.
        messages = history(c,bob,chat)
        ids = [m['id'] for m in messages]
        check(len(ids)==len(set(ids)),'duplicated message IDs in history')
        check(any(m==original for m in messages),'confirmed pre-failure message lost or altered')
        for msg in successes:
            check(any(m==msg for m in messages),'confirmed in-flight message lost or altered')
        # Bodies are unique in this scenario: an uncertain request may exist once, never twice.
        texts = [m['text'] for m in messages]
        check(len(texts)==len(set(texts)),'uncertain retry created duplicate message')
        if attachment:
            c.call('GET','/api/v1/attachments/'+attachment['id'],actor=bob,schema='Attachment')
            downloaded = c.call('GET','/api/v1/attachments/'+attachment['id']+'/content',actor=bob)
            check(downloaded['bytes']==content,'attachment lost or changed across failure')
        print('PASS: '+args.mode+' preserved confirmed data and existing session. Hook scope needs operator evidence.')
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

if __name__=='__main__':
    run(main)
