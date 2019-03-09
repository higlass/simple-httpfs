from errno import EIO, ENOENT
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from stat import S_IFDIR, S_IFREG
from threading import Timer
from time import time

import functools as ft
import logging
import os
import os.path as op
import requests
import sys
import traceback
import re

BLOCK_SIZE = 2 ** 16

CLEANUP_INTERVAL = 60
CLEANUP_EXPIRED = 60

DISK_CACHE_SIZE_ENV = 'HTTPFS_DISK_CACHE_SIZE'
DISK_CACHE_DIR_ENV = 'HTTPFS_DISK_CACHE_DIR'

import collections
import diskcache as dc

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def __getitem__(self, key):
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def __contains__(self, key):
        return key in self.cache

    def __len__(self):
        return len(self.cache)

class HttpFs(LoggingMixIn, Operations):
    """
    A read only http/https/ftp filesystem.

    """
    SSL_VERIFY = os.environ.get('SSL_VERIFY', True) not in [0, '0', False, 'false', 'False', 'FALSE', 'off', 'OFF']
    def __init__(self, schema, disk_cache_size=2**30, disk_cache_dir='/tmp/xx', lru_capacity=400):
        if not self.SSL_VERIFY:
            logging.warning('You have set ssl certificates to not be verified. This may leave you vulnerable. http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification')
        self.lru_cache = LRUCache(capacity=lru_capacity)
        self.lru_attrs = LRUCache(capacity=lru_capacity)
        self.schema = schema

        self.disk_cache = dc.Cache(disk_cache_dir, disk_cache_size)

        self.lru_hits = 0
        self.lru_misses = 0

        self.disk_hits = 0
        self.disk_misses = 0

    def getSize(self, url):
        try:
            head = requests.head(url, allow_redirects=True, verify=self.SSL_VERIFY)
            return int(head.headers['Content-Length'])
        except:
            head = requests.get(url, allow_redirects=True, verify=self.SSL_VERIFY, headers={
                'Range': 'bytes=0-1'
            })
            crange = head.headers['Content-Range']
            match = re.search(r'/(\d+)$', crange)
            if match:
                return int(match.group(1))

            logging.error(traceback.format_exc())
            raise FuseOSError(ENOENT)

    def getattr(self, path, fh=None):
        #logging.info("attr path: {}".format(path))

        if path in self.lru_attrs:
            return self.lru_attrs[path]

        if path == '/':
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
            return self.lru_attrs[path]

        if path[-2:] != '..':
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)


        url = '{}:/{}'.format(self.schema, path[:-2])

        # logging.info("attr url: {}".format(url))
        size = self.getSize(url)

        # logging.info("head: {}".format(head.headers))
        # logging.info("status_code: {}".format(head.status_code))
        # print("url:", url, "head.url", head.url)

        if size is not None:
            self.lru_attrs[path] = dict(
                st_mode=(S_IFREG | 0o644),
                st_nlink=1,
                st_size=size,
                st_ctime=time(),
                st_mtime=time(),
                st_atime=time())
        else:
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        return self.lru_attrs[path]

    def read(self, path, size, offset, fh):
        #logging.info("read path: {}".format(path))
        if path in self.lru_attrs:
            url = '{}:/{}'.format(self.schema, path[:-2])

            logging.info("read url: {}".format(url))
            logging.info("offset: {} - {} block: {}".format(offset, offset + size - 1, offset // 2 ** 18))
            output = [0 for i in range(size)]

            t1 = time()

            # nothing fetched yet
            last_fetched = -1
            curr_start = offset

            while last_fetched < offset + size:
                #print('curr_start', curr_start)
                block_num = curr_start // BLOCK_SIZE
                block_start = BLOCK_SIZE * (curr_start // BLOCK_SIZE)

                #print("block_num:", block_num, "block_start:", block_start)
                block_data = self.get_block(url, block_num)

                data_start = curr_start - (curr_start // BLOCK_SIZE) * BLOCK_SIZE
                data_end = min(BLOCK_SIZE, offset + size - block_start)

                data = block_data[data_start:data_end]

                #print("data_start:", data_start, data_end, data_end - data_start)
                for (j,d) in enumerate(data):
                    output[curr_start-offset+j] = d

                last_fetched = curr_start + (data_end - data_start)
                curr_start += (data_end - data_start)

            t2 = time()

            # logging.info("sending request")
            # logging.info(url)
            # logging.info(headers)
            logging.info("lru hits: {} lru misses: {} disk hits: {} disk misses: {}"
                    .format(self.lru_hits, self.lru_misses, self.disk_hits, self.disk_misses))

            logging.info("time: {:.2f}".format(t2 - t1))
            return bytes(output)

        else:
            logging.info("file not found: {}".format(path))
            raise FuseOSError(EIO)

    def destroy(self, path):
        pass

    def get_block(self, url, block_num):
        '''
        Get a data block from a URL. Blocks are 256K bytes in size

        Parameters:
        -----------
        url: string
            The url of the file we want to retrieve a block from
        block_num: int
            The # of the 256K'th block of this file
        '''
        cache_key=  "{}.{}".format(url, block_num)
        cache = self.disk_cache

        if cache_key in self.lru_cache:
            self.lru_hits += 1
            return self.lru_cache[cache_key]
        else:
            self.lru_misses += 1

            if cache_key in self.disk_cache:
                self.disk_hits += 1
                block_data = self.disk_cache[cache_key]
                self.lru_cache[cache_key] = block_data
                return block_data
            else:
                self.disk_misses += 1
                block_start = block_num * BLOCK_SIZE

                headers = {
                    'Range': 'bytes={}-{}'.format(block_start, block_start + BLOCK_SIZE - 1)
                }
                r = requests.get(url, headers=headers, verify=self.SSL_VERIFY)
                block_data = r.content
                self.lru_cache[cache_key] = block_data
                self.disk_cache[cache_key] = block_data

        return block_data
