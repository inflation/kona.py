import os
import itertools
import glob
import argparse
import asyncio

import requests
import uvloop
import aiohttp
import async_timeout
import ujson
from lxml import etree

import tqdm

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
requests.models.json = ujson

BASE_URL = 'http://konachan.com/post.json'


class Crawler:
    def __init__(self,
                 base_url,
                 loop=asyncio.get_event_loop(),
                 image_path=os.path.join(".", "images"),
                 image_kind="file",
                 timeout=300,
                 max_workers=4,
                 buffer_size=128):
        self.base_url = base_url
        self.loop = loop
        self.image_path = image_path
        self.session = aiohttp.ClientSession(loop=self.loop, trust_env=True)
        self.timeout = timeout
        self.max_workers = max_workers
        self.urls_queue = asyncio.Queue(buffer_size)
        self.seen_md5 = set()
        self.image_kind = image_kind + '_url'

        posts = etree.fromstring(
            requests.get(self.base_url.replace('json', 'xml')).content)
        for post in posts.xpath('//post'):
            self.urls_queue.put_nowait((post.get('md5'),
                                        post.get(self.image_kind)))

        self.pbar = tqdm.tqdm(total=int(posts.get('count')), unit='img')

    async def fetch(self, url):
        for page in itertools.count(2):
            with async_timeout.timeout(self.timeout):
                async with self.session.get(
                        url + '&page={}'.format(page)) as response:
                    posts = await response.json(loads=ujson.loads)
                    if not posts:
                        break

                    for post in posts:
                        await self.urls_queue.put((post['md5'],
                                                   post[self.image_kind]))

                    # print('Page {}'.format(page))

    async def get_image(self):
        while True:
            md5, url = await self.urls_queue.get()
            if not md5 or md5 in self.seen_md5:
                self.urls_queue.task_done()
                self.pbar.update()
                continue

            name = md5 + "." + url.split('.')[-1]
            filepath = os.path.join(self.image_path, name)
            if glob.glob(os.path.join(self.image_path, md5) + "*"):
                # print("File exists: {}, skipping".format(name))
                self.seen_md5.add(md5)
                self.urls_queue.task_done()
                self.pbar.update()
                continue

            with async_timeout.timeout(self.timeout):
                async with self.session.get(url) as response:
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    # print(name)
                    self.seen_md5.add(md5)
                    self.urls_queue.task_done()
                    self.pbar.update()

    async def crawl(self):
        url_worker = self.loop.create_task(self.fetch(self.base_url))
        workers = [
            self.loop.create_task(self.get_image())
            for _ in range(self.max_workers)
        ]

        await url_worker
        await self.urls_queue.join()

        for w in workers:
            w.cancel()
        url_worker.cancel()

    def close(self):
        self.pbar.close()
        self.session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Crawler for konachan")
    parser.add_argument(
        'tags',
        nargs='+',
        help='Specify which tags you want to search, can be multiple')
    parser.add_argument(
        '-o', '--output', default='images', help='Output directory')
    parser.add_argument(
        '-r',
        '--rating',
        default='safe',
        choices=[
            'safe', 'explicit', 'questionable', 'questionableless',
            'questionableplus'
        ],
        help='Degree of Hentainess')
    parser.add_argument(
        '-w',
        '--workers',
        default=4,
        type=int,
        help='Number of image downloaders')
    parser.add_argument(
        '-t',
        '--timeout',
        default=300,
        type=int,
        help='Timeout before disconnect')
    parser.add_argument(
        '-b', '--buffer', default=64, type=int, help='Size of url queue')
    parser.add_argument(
        '-k',
        '--kind',
        default='file',
        choices=['preview', 'file', 'sample', 'jpeg'],
        help='Kind of image you want to download. "file" for original,'
        '"sample" for small sized version')

    args = parser.parse_args()
    request_url = BASE_URL + "?tags={}+rating:{}&limit={}".format(
        '+'.join(args.tags), args.rating, args.buffer)
    print(request_url)

    folder_path = os.path.join('.', args.output)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    main_loop = asyncio.get_event_loop()
    crawler = Crawler(
        request_url,
        image_path=folder_path,
        image_kind=args.kind,
        loop=main_loop,
        max_workers=args.workers,
        timeout=args.timeout,
        buffer_size=args.buffer)

    try:
        main_loop.run_until_complete(crawler.crawl())
    finally:
        crawler.close()
        main_loop.stop()
        main_loop.close()
