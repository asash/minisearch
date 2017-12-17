#/usr/bin/env python3
import requests
from requests.exceptions import ConnectionError
import os
import aws
import config
import sys
import json
from hashlib import sha256
from utils import log

from urllib.parse import urljoin, urlparse, quote
from lxml.html import etree
from lxml.etree import XMLSyntaxError
from utils import mkdir_p
from init_robots import init_robots

import time

START_URLS = ['https://www.habrahabr.ru/']
ALLOWED_DOMAINS={'habrahabr.ru', 'geektimes.ru'}
TIMEOUT=2.5
MAX_RETRY = 10
PULL_N_URLS = 10
SYNC_DUMPED_EVERY = 60




def get_html(http_response):
    return(http_response.text)

def get_links(html):
    parser = etree.HTMLParser()
    
    try:
        tree = etree.fromstring(html, parser=parser)
    except XMLSyntaxError as ex:
        log.warn('html parsing error')
        return []

    if tree is None:
        log.warn("html not parsed")
        return []
    links = tree.xpath('//a/@href')
    return links

robots_crawl_prohibited = init_robots(ALLOWED_DOMAINS)

def normalize_links(current_url, links):
    result = [normalize_link(current_url, link) for link in links]
    return result

def normalize_link(current_url, link):
    url_with_tail = urljoin(current_url, link) 
    normalized = remove_tail(url_with_tail)
    return normalized

def remove_tail(url):
    parsed = urlparse(url)
    result = parsed.scheme + '://' + parsed.netloc + parsed.path
    return result

def link_domain_disallowed(url):
    parsed = urlparse(url)
    return parsed.netloc not in ALLOWED_DOMAINS

def is_image(url):
    image_suffixes = ['.png', '.jpg', 'jpeg', '.gif']
    for suffix in image_suffixes:
        if url.endswith(suffix):
            return True
    return False

def is_habr_qa(url):
    return urlparse(url).path.startswith('/qa/')

def habr_max_depth(url):
    splits = urlparse(url).path.split('/')
    return len(splits) > 8

def habr_not_slashed(url):
    return not(url.endswith('/'))

def habr_user_limit(url):
    path = urlparse(url).path
    if path.startswith('/users/'):
        splits = path.split('/')
        if splits[-2].startswith('page'):
            try:
                n = int(splits[-2][len('page'):])
                if n >= 10:
                    log.info('url: {}: user pages limit'.format(url))
                    return True
                else:
                    return False
            except ValueError:
                log.info('incorrect habra url: {}'.format(url))
                return True
        else:
            return False
    else:
        return False



def filter_urls(filters, urls):
    if len(filters) == 0:
        return urls
    else:
        return filter(lambda x: not filters[0](x), filter_urls(filters[1:], urls))

def get_response(url):
    for i in range(MAX_RETRY):
        try:
            return requests.get(url, timeout=TIMEOUT)
        except Exception as ex:
            print("cannot crawl url {} by reason {}. retry in 1 sec".format(url, ex))
            time.sleep(1)
    return requests.Response()


def get_filters():
    return [
	    link_domain_disallowed,
	    robots_crawl_prohibited,
	    is_image,
            is_habr_qa,
            habr_not_slashed,
	    habr_user_limit, 
            habr_max_depth
           ]

def check_response(response):
    return (response.status_code == 200) and\
           (response.headers['Content-Type'].startswith('text/html')) 

def get_filtered_links(url, html):
    links = get_links(html)
    normalized_urls = normalize_links(url, links)
    filters = get_filters()
    filtered_urls = list(filter_urls(filters, normalized_urls))
    return set(filtered_urls)
   
def save_html(url, html):
    urlhash = sha256(url.encode()).hexdigest()
    domain = urlparse(url).netloc
    dir_prefix = urlhash[:2]
    filename = "{}/{}/{}".format(domain, dir_prefix, urlhash)
    data = json.dumps({'url': url, 'text': html}).encode()
    aws.bytes_to_s3(data, config.AWS_RESULTS_BUCKET, filename)


def process_url(url):
    filters = get_filters()
    if len(list(filter_urls(filters, [url]))) == 0:
        log.info('url {} is filtered, skipping processing'.format(url))
        return []
    log.info('processing url ' + url)
    http_response = get_response(url)
    if not (check_response(http_response)):
        return []
    html = get_html(http_response)
    save_html(url, html)
    links = get_filtered_links(url, html) 
    return links


def mainloop(queue):
    while True:
        url = queue.pull()
        try:
            if url is None:
                log.info("nothing to process")
                time.sleep(1)
                continue
            links = process_url(url)
            queue.push(links)
        except Exception as ex:
            log.error('cannot process url {}: {}, {}'.format(url, type(ex), ex))
            
            

class UrlQueueClient(object):
    def __init__(self, api_url):
        self.api_url = api_url
        self.queue_buff = []
        self.added_urls = set() 
        self.last_sync = -1

    def check_sync(self):
        if time.time() - self.last_sync >= SYNC_DUMPED_EVERY:
            try:
                obj = aws.object_from_s3(config.AWS_QUEUE_BUCKET, 'added_urls')
            #TODO fix to correct exception type
            except Exception as ex:
                log.error('cannot extract queue dump from s3: {}, {}'.format(type(ex), ex))
                return

            if obj is not None:
                self.added_urls = obj
                log.info('added urls succesfuly sunced, size of added_urls is {}'.format(len(self.added_urls)))
            else:
                log.info('added_urls dump does not exists')
            self.last_sync = time.time()

             

    def get_response(self, url):
        while True:
            try:
                response = requests.get(url)
                break
            except requests.exceptions.ConnectionError:
                log.info('queue is not available. sleep for 1 sec')
                time.sleep(1)
        return response

    def post_response(self, url, data):
        while True:
            try:
                data_encoded = data.encode('utf8') 
                response = requests.post(url, data_encoded)
                break
            except requests.exceptions.ConnectionError:
                log.info('queue is not available. sleep for 1 sec')
                time.sleep(1)
        return response

        

    def push(self, urls):
        self.check_sync()
        urls_to_add = list(filter(lambda x: x.encode() not in self.added_urls, urls)) 
        log.info('adding {} urls of {}'.format(len(urls_to_add), len(urls)))
        if len(urls_to_add) == 0:
            return

        request_url = self.api_url + '/push/'  
        response = self.post_response(request_url, '\n'.join(urls_to_add))
        if response.status_code != 204:
            raise Exception('Problem with queue. Response code should be equal 204 for push request')

    def pull(self):
        if len(self.queue_buff) == 0:
            if not(self.pull_batch()):
                return None
        
        result = self.queue_buff[0] 
        del(self.queue_buff[0])
        return result

    def pull_batch(self):
        request_url = self.api_url + '/pull/{}'.format(PULL_N_URLS)  
        response = self.get_response(request_url)
        if response.status_code == 204:
            return False
            log.info('queue is empty'.format(len(self.queue_buff)))

        elif response.status_code == 200:
            self.queue_buff = [url.strip() for url in response.text.split('\n')]
            log.info('{} urls successfuly pulled'.format(len(self.queue_buff)))
            return True


        else:
            raise Exception('Problem with queue. invalid response code for pull request')
 

if __name__ == '__main__':
    mkdir_p ('./results')
    queue = UrlQueueClient(sys.argv[1])
    queue.push(START_URLS)
    mainloop(queue)
