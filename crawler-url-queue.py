from tornado.locks import Lock
from  urllib.parse import urlparse, parse_qs
import tornado.ioloop
import pickle
import time
import sys
import tornado.web
import config
from utils import log, mkdir_p
import aws
from collections import defaultdict
import graphyte

lock = Lock()
graphyte.init('localhost', prefix='minisearch', interval=10)

class UrlQueue(object):
    def __init__(self):
        self.added_urls = set()
        self.queue = []
        self.pulled_cnt = 0
        self.op_cnt = 0
        self.last_save = 0
        self.last_stat = 0 
        self.ip_stats = defaultdict(lambda:defaultdict(lambda:0))
    
    def send_stats(self):
        graphyte.send('pulled_cnt', self.pulled_cnt)
        graphyte.send('queue_size', len(self.queue))
        graphyte.send('added_cnt', len(self.added_urls))
        for ip in self.ip_stats:
            for metric in self.ip_stats[ip]:
                graphyte.send("{}.{}".format(ip, metric), self.ip_stats[ip][metric])
        self.last_stat = time.time()
        log.info('GRAPHITE: stats send to graphite')
    
    def dump_check(self, n=1):
        if (time.time() - self.last_stat) > config.QUEUE_STATS_EVERY_SEC:
            self.send_stats()

        self.op_cnt += n 
        if self.op_cnt - self.last_save >= config.QUEUE_DUMP_EVERY_OPS:
            self.dump()

    def dump(self):
        log.info('dumping added url set...')
        aws.object_to_s3(self.added_urls, config.AWS_QUEUE_BUCKET, 'added_urls')
        self.last_save = self.op_cnt
            


    def sync_pull(self, n, ip):
        if len(self.queue) > 0: 
            result = self.queue[:n] 
            self.queue = self.queue[n:]
            self.pulled_cnt += n 
            self.ip_stats[ip]['pulled_cnt'] += n 
            log.info('PULL: {} urls successfully pulled from the queue. queue size is {}, total_pulled:{}'.format(len(result), len(self.queue), self.pulled_cnt))
            self.dump_check()
            return result
        else: 
            log.info('pull fail: queue is empty')
            return None

    def sync_push(self, urls, ip):
        added_cnt = 0
        filtered_cnt = 0

        for url in urls:
            if url not in self.added_urls:
                self.added_urls.add(url)
                self.queue.append(url)
                added_cnt += 1
                self.ip_stats[ip]['pushed_cnt'] += 1 
            else:
                filtered_cnt += 1
        log.info('PUSH: {} urls successfully pushed to queue and {} filtered. queue size is {}'.format(added_cnt, filtered_cnt, len(self.queue)))
        self.dump_check(added_cnt)

    async def pull(self, n, ip):
        async with lock: 
            result = self.sync_pull(n, ip)
            return result
       
    async def push(self, urls, ip):
        async with lock: 
            self.sync_push(urls, ip)



class PushHandler(tornado.web.RequestHandler):
    async def post(self):
        ip = self.request.remote_ip
        query = urlparse(self.request.uri).query
        urls_bytes = self.request.body
        urls = urls_bytes.split(b'\n')
        result = await queue.push(urls, ip)
        self.set_status(204)
        self.finish()

class PullHandler(tornado.web.RequestHandler):
    async def get(self):
        ip = self.request.remote_ip
        n = int(self.request.uri.split('/')[-1])
        urls = await queue.pull(n, ip)
        if urls is not None:
            self.set_status(200)
            self.write(b'\n'.join(urls))
        else:
            self.set_status(204)
        self.finish()

def make_app():
    return tornado.web.Application([
            (r"/push/", PushHandler),
            (r"/pull/.*", PullHandler),
    ])

if __name__ == "__main__":
    queue = UrlQueue() 
    queue.dump()

    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
