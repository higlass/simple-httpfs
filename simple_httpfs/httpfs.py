import collections
import logging
import numpy as np
import os
import os.path as op
import requests
import sys
import traceback
import re
from errno import EIO, ENOENT
from stat import S_IFDIR, S_IFREG
from threading import Timer
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import diskcache as dc

from ftplib import FTP
from urllib.parse import urlparse

CLEANUP_INTERVAL = 60
CLEANUP_EXPIRED = 60


DISK_CACHE_SIZE_ENV = "HTTPFS_DISK_CACHE_SIZE"
DISK_CACHE_DIR_ENV = "HTTPFS_DISK_CACHE_DIR"


FALSY = {0, "0", False, "false", "False", "FALSE", "off", "OFF"}


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


class FtpFetcher:
    def server_path(self, url):
        o = urlparse(url)

        return (o.netloc, o.path)

    def login(self, server):
        ftp = FTP(server)
        ftp.login()

        return ftp

    def get_size(self, url):
        (server, path) = self.server_path(url)

        ftp = self.login(server)
        size = ftp.size(path)
        ftp.close()
        return size

    def get_data(self, url, start, end):
        import time

        (server, path) = self.server_path(url)
        t1 = time.time()
        print("getting: {}-{}", start, end)

        ftp = self.login(server)
        conn = ftp.transfercmd("RETR {}".format(path), rest=start)

        amt = end - start
        chunk_size = 1 << 15
        data = []
        while len(data) < amt:
            chunk = conn.recv(chunk_size)
            if chunk:
                data += chunk
            else:
                break
        if len(data) < amt:
            data += [0] * (amt - len(data))
        else:
            data = data[:amt]

        ftp.close()
        t2 = time.time()
        print(t2 - t1)
        return np.array(data, dtype=np.uint8)


class HttpFetcher:
    SSL_VERIFY = os.environ.get("SSL_VERIFY", True) not in FALSY

    def __init__(self):
        if not self.SSL_VERIFY:
            logging.warning(
                "You have set ssl certificates to not be verified. "
                "This may leave you vulnerable. "
                "http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification"
            )

    def get_size(self, url):
        try:
            head = requests.head(url, allow_redirects=True, verify=self.SSL_VERIFY)
            return int(head.headers["Content-Length"])
        except:
            head = requests.get(
                url,
                allow_redirects=True,
                verify=self.SSL_VERIFY,
                headers={"Range": "bytes=0-1"},
            )
            crange = head.headers["Content-Range"]
            match = re.search(r"/(\d+)$", crange)
            if match:
                return int(match.group(1))

            logging.error(traceback.format_exc())
            raise FuseOSError(ENOENT)

    def get_data(self, url, start, end):
        headers = {"Range": "bytes={}-{}".format(start, end), "Accept-Encoding": ""}
        r = requests.get(url, headers=headers)
        block_data = np.frombuffer(r.content, dtype=np.uint8)
        return block_data


class HttpFs(LoggingMixIn, Operations):
    """
    A read only http/https/ftp filesystem.

    """

    def __init__(
        self,
        schema,
        disk_cache_size=2 ** 30,
        disk_cache_dir="/tmp/xx",
        lru_capacity=400,
        block_size=2 ** 20,
    ):
        self.lru_cache = LRUCache(capacity=lru_capacity)
        self.lru_attrs = LRUCache(capacity=lru_capacity)
        self.schema = schema

        if schema == "http" or schema == "https":
            self.fetcher = HttpFetcher()
        elif schema == "ftp":
            self.fetcher = FtpFetcher()
        else:
            raise ("Unknown schema: {}".format(schema))

        self.disk_cache = dc.Cache(disk_cache_dir, disk_cache_size)

        self.lru_hits = 0
        self.lru_misses = 0

        self.disk_hits = 0
        self.disk_misses = 0
        self.block_size = block_size

    def getSize(self, url):
        return self.fetcher.get_size(url)

    def getattr(self, path, fh=None):
        # logging.info("attr path: {}".format(path))

        if path in self.lru_attrs:
            return self.lru_attrs[path]

        if path == "/":
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
            return self.lru_attrs[path]

        if path[-2:] != "..":
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        url = "{}:/{}".format(self.schema, path[:-2])

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
                st_atime=time(),
            )
        else:
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        return self.lru_attrs[path]

    def read(self, path, size, offset, fh):
        # logging.info("read path: {}".format(path))
        if path in self.lru_attrs:
            url = "{}:/{}".format(self.schema, path[:-2])

            logging.info("read url: {}".format(url))
            logging.info(
                "offset: {} - {} request_size (KB): {:.2f} block: {}".format(
                    offset, offset + size - 1, size / 2 ** 10, offset // self.block_size
                )
            )
            output = np.zeros((size,), np.uint8)

            t1 = time()

            # nothing fetched yet
            last_fetched = -1
            curr_start = offset

            while last_fetched < offset + size:
                block_num = curr_start // self.block_size
                block_start = self.block_size * (curr_start // self.block_size)

                block_data = self.get_block(url, block_num)

                data_start = (
                    curr_start - (curr_start // self.block_size) * self.block_size
                )

                data_end = min(self.block_size, offset + size - block_start)
                data = block_data[data_start:data_end]

                d_start = curr_start - offset
                output[d_start : d_start + len(data)] = data

                last_fetched = curr_start + (data_end - data_start)
                curr_start += data_end - data_start

            t2 = time()

            # logging.info("sending request")
            # logging.info(url)
            # logging.info(headers)
            logging.info(
                "lru hits: {} lru misses: {} disk hits: {} disk misses: {}".format(
                    self.lru_hits, self.lru_misses, self.disk_hits, self.disk_misses
                )
            )

            logging.info("time: {:.4f}".format(t2 - t1))
            return bytes(output)

        else:
            logging.info("file not found: {}".format(path))
            raise FuseOSError(EIO)

    def destroy(self, path):
        self.disk_cache.close()

    def get_block(self, url, block_num):
        """
        Get a data block from a URL. Blocks are 256K bytes in size

        Parameters:
        -----------
        url: string
            The url of the file we want to retrieve a block from
        block_num: int
            The # of the 256K'th block of this file
        """
        cache_key = "{}.{}.{}".format(url, self.block_size, block_num)
        cache = self.disk_cache

        if cache_key in self.lru_cache:
            self.lru_hits += 1
            hit = self.lru_cache[cache_key]
            return hit
        else:
            self.lru_misses += 1

            if cache_key in self.disk_cache:
                try:
                    block_data = self.disk_cache[cache_key]
                    self.disk_hits += 1
                    self.lru_cache[cache_key] = block_data
                    return block_data
                except KeyError:
                    pass

            self.disk_misses += 1
            block_start = block_num * self.block_size

            block_data = self.fetcher.get_data(
                url, block_start, block_start + self.block_size - 1
            )

            self.lru_cache[cache_key] = block_data
            self.disk_cache[cache_key] = block_data

        return block_data
