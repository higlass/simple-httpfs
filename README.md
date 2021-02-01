# simple-httpfs

[![PyPI](https://img.shields.io/pypi/v/simple-httpfs.svg?logo=python&logoColor=white)](https://pypi.org/project/simple-httpfs/)

A simple FUSE-based http file system. Read http files as if they were on
the local filesystem.

## Usage

```
simple-http /my/mount/dir
curl /my/mount/dir/http/slashdot.org/country.js..
```

URLs are referenced relative to the mount directory and suffixed with `..` in
the style of [Daniel Rozenbergs
httpfs](https://github.com/danielrozenberg/httpfs).

## Unmounting

```
umount /my/mount/dir
```
