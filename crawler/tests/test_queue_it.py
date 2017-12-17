from unittest import TestCase
import unittest
import subprocess
import requests
import shlex

class TestQueueIT(TestCase):

    def test_queue(self):
        urls = ['a', 'b', 'c', 'd']
        for url in urls:
            requests.get('http://localhost:8888/push/?url=' + url)

        pulled = []
        while True:
            request = requests.get('http://localhost:8888/pull/') 
            if request.status_code != 200:
                break
            pulled.append(request.text)
        self.assertEquals(pulled, urls)
        

if __name__ == '__main__':
    unittest.main()
