import threading
from concurrent.futures import ThreadPoolExecutor
from client import Client, arguments, check, run

def main():
    args = arguments('Concurrency race test')
    bootstrap = Client(args.base_url, args.ca_file)
    alice, bob, _eve, chat = bootstrap.bootstrap()
    chat_id = chat['id']

    CLIENTS = 10
    PER_CLIENT = 10
    barrier = threading.Barrier(CLIENTS) # Барьер: все потоки стартуют одновременно

    def worker(i):
        local = Client(args.base_url, args.ca_file)
        barrier.wait() # Ждем, пока все 10 потоков не будут готовы
        for j in range(PER_CLIENT):
            local.call('POST', f'/api/v1/chats/{chat_id}/messages', 201, actor=alice, body={'text': f'w{i}-{j}'})

    # Запускаем 10 потоков
    with ThreadPoolExecutor(max_workers=CLIENTS) as pool:
        list(pool.map(worker, range(CLIENTS)))

    # Читаем историю и считаем уникальные ID
    seen = []
    cursor = None
    while True:
        path = f'/api/v1/chats/{chat_id}/messages?limit=100'
        if cursor: path += '&cursor=' + cursor
        page = bootstrap.call('GET', path, actor=bob, schema='MessagePage')['json']
        seen.extend(x['id'] for x in page['items'])
        cursor = page['next_cursor']
        if not cursor: break

    check(len(seen) == 100, f'Ожидали 100, получили {len(seen)}')
    check(len(set(seen)) == 100, 'Есть дубликаты ID!')
    print('PASS: 10x10 concurrent writes -> 100 unique ids')