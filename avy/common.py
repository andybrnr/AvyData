"""
@author: MPeters, ABerner
"""

import json
import requests
import time
import pandas as pd

from gzip import GzipFile


class JsonFile(object):
    '''
    A text file where each line is one deserialized json string

    Provides iterator access to read and write from the file.

    # for reading a file
    with JsonFile('input_file.json', 'r') as fin:
        for line in fin:
            # do something with line
            pass

    # for writing
    with JsonFile('output_file.json', 'w') as fout:
        fout.write({'url': 'http://www.nwac.us/archive/2017-01-05,
                    'content': 'HTML content here'})
        fout.write({'url': 'http://www.nwac.us/archive/2017-01-06,
                    'content': 'HTML content here'})
    '''
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __iter__(self):
        for line in self._file:
            yield json.loads(line)

    def write(self, item):
        item_as_json = json.dumps(item, ensure_ascii=False)
        encoded = '{0}\n'.format(item_as_json)
        self._file.write(encoded)

    def __enter__(self):
        self._file = open(*self._args, **self._kwargs)
        self._file.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.__exit__(exc_type, exc_val, exc_tb)


class GzipJsonFile(JsonFile):
    '''
    A gzip compressed JsonFile.  Usage is the same as JsonFile
    '''
    def __enter__(self):
        self._file = GzipFile(*self._args, **self._kwargs)
        self._file.__enter__()
        return self

    def write(self, item):
        item_as_json = json.dumps(item, ensure_ascii=False)
        encoded = '{0}\n'.format(item_as_json).encode('utf-8', 'ignore')
        self._file.write(encoded)


def retry(func, args, kwargs, initial_wait=1.0, max_retries=5):
    '''
    Call the function with retries and exponential backoff

    func: a callable func(args, kwargs)
    '''
    n = 0
    wait = initial_wait
    while True:
        try:
            ret = func(*args, **kwargs)
            break
        except:
            n += 1
            if n > max_retries:
                raise
            print("Retry {0}, waiting {1}".format(n, wait))
            time.sleep(wait)
            wait *= 2
    return ret


class DataFetcher(object):
    '''
    Fetch pages and save them to disk

    Takes a list of URLs, fetches, and saves to disk
    '''
    def __init__(self, sleep_interval=1):
        self.sleep_interval = sleep_interval

    def fetch_pages(self, urls, outfile):
        '''
        Given a list of URL strings, fetch the content and save to the file
        '''
        with GzipJsonFile(outfile, 'w') as fout:
            for u in urls:
                response = retry(requests.get, (u,), {})
                line = {
                    'url': u,
                    'time': pd.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'status': response.status_code,
                    'content': response.text
                }
                fout.write(line)
                print("Fetched {0}, length={1}".format(u, len(response.text)))
                time.sleep(self.sleep_interval)


