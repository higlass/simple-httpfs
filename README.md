# simple-httpfs

A simple FUSE-based http/object storage file system. Read remote files as if they were on the local filesystem.

## Usage

```
simple-httpfs /my/mount/dir
cat /my/mount/dir/http://slashdot.org/country.js...
```

Fully qualified URLs are referenced relative to the mount directory and distinguished from directories by appending a shell-safe trailing sentinel string (default: `...`) in
the style of [Daniel Rozenbergs httpfs](https://github.com/danielrozenberg/httpfs).

## Unmounting

Use `umount` or `fusermount`.

```
umount /my/mount/dir
```
