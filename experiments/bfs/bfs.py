#/usr/bin/env python3
import requests
from requests.exceptions import ConnectionError
import os
import sys
import json
from queue import Queue
from hashlib import sha256

from urllib.parse import urljoin, urlparse, quote
from lxml.html import etree
from lxml.etree import XMLSyntaxError

import time

START_URL = 'https://www.habrahabr.ru/'
ALLOWED_DOMAINS={'habrahabr.ru', 'geektimes.ru'}
TIMEOUT=2.5
MAX_RETRY = 10

def get_html(http_response):
    return(http_response.text)

def get_links(html):
    parser = etree.HTMLParser()
    
    try:
        tree = etree.fromstring(html, parser=parser)
    except XMLSyntaxError as ex:
        return []

    if tree is None:
        return []
    links = tree.xpath('//a/@href')
    return links


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
                    return True
                else:
                    return False
            except ValueError:
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
    data = json.dumps({'url': url, 'text': html})
    f = open('./results/{}.json'.format(urlhash), 'w')
    f.write(data)
    f.close()



def get(url):
    http_response = get_response(url)
    html = get_html(http_response)
    return html
   	
def bfs(start_url):
    queue = Queue()
    queue.put(start_url)
    seen_links = {start_url} 
    
    while not (queue.empty()):
        url = queue.get()
        print('processing url ' + url)
        html = get(url)
        save_html(url, html)
        for link in get_filtered_links(url, html):
            if link not in seen_links:
                queue.put(link)
                seen_links.add(link)

bfs(START_URL)
