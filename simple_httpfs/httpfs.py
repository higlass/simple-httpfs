import collections
import logging
import os
import os.path as op
import re
import sys
import traceback
from errno import EIO, ENOENT
from ftplib import FTP
from stat import S_IFDIR, S_IFREG
from threading import Timer
from time import sleep, time
from urllib.parse import urlparse

import boto3
import diskcache as dc
import numpy as np
import requests
from fuse import FUSE, FuseOSError, LoggingMixIn, Operations
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
    wait_random,
)

import slugid

CLEANUP_INTERVAL = 60
CLEANUP_EXPIRED = 60

REPORT_INTERVAL = 60

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

        try:
            # do a retrbinary on a non-existent file
            # to set the transfer mode to binary
            # use a dummy callback too
            ftp.retrbinary(slugid.nice(), lambda x: x + 1)
        except:
            pass

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
        return np.array(data, dtype=np.uint8)


def is_403(value):
    """Return True if the error is a 403 exception"""
    return value is not None


class HttpFetcher:
    SSL_VERIFY = os.environ.get("SSL_VERIFY", True) not in FALSY

    def __init__(self, logger):
        self.logger = logger
        if not self.SSL_VERIFY:
            logger.warning(
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

            self.logger.error(traceback.format_exc())
            raise FuseOSError(ENOENT)

    @retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(2))
    def get_data(self, url, start, end):
        headers = {"Range": "bytes={}-{}".format(start, end), "Accept-Encoding": ""}
        self.logger.info("getting %s %s %s", url, start, end)
        r = requests.get(url, headers=headers)
        self.logger.info("got %s", r.status_code)

        r.raise_for_status()
        block_data = np.frombuffer(r.content, dtype=np.uint8)
        return block_data


class S3Fetcher:
    SSL_VERIFY = os.environ.get("SSL_VERIFY", True) not in FALSY

    def __init__(self, aws_profile, logger):
        self.logger = logger
        self.logger.info("Creating S3Fetcher with aws_profile=%s", aws_profile)
        self.session = boto3.Session(profile_name=aws_profile)
        self.client = self.session.client("s3")
        pass

    def parse_bucket_key(self, url):
        url_parts = urlparse(url, allow_fragments=False)
        bucket = url_parts.netloc
        key = url_parts.path.strip("/")

        return bucket, key

    def get_size(self, url):
        bucket, key = self.parse_bucket_key(url)

        response = self.client.head_object(Bucket=bucket, Key=key)
        size = response["ContentLength"]
        return size

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_data(self, url, start, end):
        bucket, key = self.parse_bucket_key(url)
        obj = boto3.resource("s3").Object(bucket, key)
        stream = self.client.get_object(
            Bucket=bucket, Key=key, Range="bytes={}-{}".format(start, end)
        )["Body"]
        contents = stream.read()
        block_data = np.frombuffer(contents, dtype=np.uint8)
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
        aws_profile=None,
        logger=None,
    ):
        self.lru_cache = LRUCache(capacity=lru_capacity)
        self.lru_attrs = LRUCache(capacity=lru_capacity)
        self.schema = schema
        self.logger = logger
        self.last_report_time = 0
        self.total_requests = 0
        self.getting = set()

        if not self.logger:
            self.logger = logging.getLogger(__name__)

        self.logger.info("Starting with disk_cache_size: %d", disk_cache_size)

        if schema == "http" or schema == "https":
            self.fetcher = HttpFetcher(self.logger)
        elif schema == "ftp":
            self.fetcher = FtpFetcher()
        elif schema == "s3":
            self.fetcher = S3Fetcher(aws_profile, self.logger)
        else:
            raise ("Unknown schema: {}".format(schema))

        self.disk_cache = dc.Cache(disk_cache_dir, size_limit=disk_cache_size)

        self.total_blocks = 0
        self.lru_hits = 0
        self.lru_misses = 0

        self.disk_hits = 0
        self.disk_misses = 0
        self.block_size = block_size

    def getSize(self, url):
        try:
            return self.fetcher.get_size(url)
        except Exception as ex:
            self.logger.exception(ex)
            raise

    def getattr(self, path, fh=None):
        try:
            if path in self.lru_attrs:
                return self.lru_attrs[path]

            if path == "/":
                self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
                return self.lru_attrs[path]

            if (
                path[-2:] != ".."
                and not path.endswith("..-journal")
                and not path.endswith("..-wal")
            ):
                return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

            url = "{}:/{}".format(self.schema, path[:-2])

            # there's an exception for the -jounral files created by SQLite
            if not path.endswith("..-journal") and not path.endswith("..-wal"):
                size = self.getSize(url)
            else:
                size = 0

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
        except Exception as ex:
            self.logger.exception(ex)
            raise

    def unlink(self, path):
        return 0

    def create(self, path, mode, fi=None):
        return 0

    def write(self, path, buf, size, offset, fip):
        return 0

    def read(self, path, size, offset, fh):
        t1 = time()

        self.logger.debug("read %s %s %s", path, offset, size)

        if t1 - self.last_report_time > REPORT_INTERVAL:
            """
            self.logger.info(
                "lru hits: {} lru misses: {} disk hits: {} total_requests: {}".format(
                    self.lru_hits,
                    self.lru_misses,
                    self.disk_hits,
                    self.disk_misses,
                    self.total_requests,
                )
            )
            """
            pass
        try:
            self.total_requests += 1

            attr = self.getattr(path)
            url = "{}:/{}".format(self.schema, path[:-2])

            self.logger.debug("read url: {}".format(url))
            self.logger.debug(
                "offset: {} - {} request_size (KB): {:.2f} block: {}".format(
                    offset,
                    offset + size - 1,
                    size / 2 ** 10,
                    offset // self.block_size,
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

                block_id = (url, block_num)
                while block_id in self.getting:
                    sleep(0.05)

                self.getting.add(block_id)
                block_data = self.get_block(url, block_num)
                self.getting.remove(block_id)

                data_start = (
                    curr_start - (curr_start // self.block_size) * self.block_size
                )

                data_end = min(self.block_size, offset + size - block_start)
                data = block_data[data_start:data_end]

                d_start = curr_start - offset
                output[d_start : d_start + len(data)] = data

                last_fetched = curr_start + (data_end - data_start)
                curr_start += data_end - data_start

            bts = bytes(output)

            return bts

        except Exception as ex:
            self.logger.exception(ex)
            raise

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

        self.total_blocks += 1

        if cache_key in self.lru_cache:
            self.lru_hits += 1
            hit = self.lru_cache[cache_key]
            return hit
        else:
            self.lru_misses += 1

            if cache_key in self.disk_cache:
                self.logger.info("cache hit: %s", cache_key)
                try:
                    block_data = self.disk_cache[cache_key]
                    self.disk_hits += 1
                    self.lru_cache[cache_key] = block_data
                    return block_data
                except KeyError:
                    pass

            self.disk_misses += 1
            block_start = block_num * self.block_size

            self.logger.info("getting data %s", cache_key)
            block_data = self.fetcher.get_data(
                url, block_start, block_start + self.block_size - 1
            )

            self.lru_cache[cache_key] = block_data
            self.disk_cache[cache_key] = block_data

        return block_data
