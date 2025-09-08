import posixpath as pp
from collections.abc import Sequence
from datetime import datetime
from ftplib import FTP, error_perm
from urllib.parse import urlparse

from obspec import GetRange, Head, ListResult, ListWithDelimiter, ObjectMeta


class FTPStore(GetRange, Head, ListWithDelimiter):
    """
    An obspec protocol API for FTP.

    See https://developmentseed.org/obspec.
    """

    def __init__(self, url: str, chunk_size: int = 32 * 1024):
        o = urlparse(url)
        self.host = o.netloc
        self.path = o.path
        self.chunk_size = chunk_size

    def _open(self, host: str) -> FTP:
        ftp = FTP(host)
        ftp.login()

        # Set the transfer mode to binary.
        ftp.voidcmd("TYPE I")

        return ftp

    def head(self, path: str) -> ObjectMeta:
        fpath = "/".join((self.path, path)).replace("//", "/")
        ftp = self._open(self.host)

        try:
            size = ftp.size(fpath) or 0
        except error_perm as e:
            raise FileNotFoundError from e

        # 213 File Status means a modification time was returned
        try:
            resp = ftp.sendcmd(f"MDTM {fpath}")
            if resp.startswith("213"):
                last_modified = datetime.strptime(resp[4:].strip(), "%Y%m%d%H%M%S")
            else:
                last_modified = datetime.now()
        except Exception:
            last_modified = datetime.now()

        ftp.close()

        return {
            "e_tag": None,
            "last_modified": last_modified,
            "path": fpath,
            "size": size,
            "version": None,
        }

    def get_range(
        self, path: str, start: int, end: int | None = None, length: int | None = None
    ) -> bytes:
        if length is not None:
            end = start + length
        elif end is None:
            raise ValueError("Must specify either 'end' or 'length'")

        fpath = "/".join((self.path, path)).replace("//", "/")
        ftp = self._open(self.host)
        if start > (ftp.size(fpath) or 0):
            data = b""
        else:
            conn = ftp.transfercmd(f"RETR {fpath}", rest=start)
            amt = end - start

            # Fetch the data in chunks.
            data = b""
            while len(data) < amt:
                chunk = conn.recv(self.chunk_size)
                if chunk:
                    data += chunk
                else:
                    break

            # Pad with null bytes if we didn't get enough data, or trim.
            if len(data) < amt:
                data += b"\x00" * (amt - len(data))
            else:
                data = data[:amt]

        ftp.close()
        return data

    def list_with_delimiter(
        self, prefix: str | None = None
    ) -> ListResult[Sequence[ObjectMeta]]:
        dir_path = _resolve_search_dir(self.path, prefix)

        ftp = self._open(self.host)
        try:
            dir_listing = ftp.mlsd(dir_path)
        except error_perm:
            ftp.close()
            return {"common_prefixes": [], "objects": []}

        objects: list[ObjectMeta] = []
        common_prefixes: set[str] = set()

        for name, facts in dir_listing:
            if name in [".", ".."]:
                continue

            # Include the prefix in the returned path
            resolved_name = _resolve_path(name, prefix)
            if resolved_name is None:
                continue

            # Directories are listed under "common_prefixes"
            # Files are listed under "objects"
            if facts.get("type", "file") == "dir":
                common_prefixes.add(resolved_name + "/")
            else:
                size = int(facts.get("size", 0))
                modify_time = facts.get("modify")
                if modify_time is not None:
                    last_modified = _modify_time_to_datetime(modify_time)
                else:
                    last_modified = datetime.now()
                objects.append(
                    {
                        "e_tag": None,
                        "last_modified": last_modified,
                        "path": resolved_name,
                        "size": size,
                        "version": None,
                    }
                )

        ftp.close()

        return {"common_prefixes": sorted(list(common_prefixes)), "objects": objects}


def _resolve_search_dir(path: str, prefix: str | None) -> str:
    """
    Resolve the directory to search given ``prefix``.

    If the prefix ends with '/', assume it's a subdirectory of 'path'.
    We will search and return all of its contents.

    If prefix doesn't end with '/', assume it's an object/file prefix.
    We will search its parent and filter the results for matches.
    """
    if prefix is not None:
        # Trim back 'prefix' as needed and prepend the base path.
        # If 'prefix' does not end with '/' it will get trimmed back to the
        # parent of the last component.
        parent_dir = pp.dirname(prefix)
        if parent_dir:
            path = pp.join(path, parent_dir)

    return pp.normpath(path)


def _resolve_path(name: str, prefix: str | None) -> str | None:
    """
    Resolve the path of a retrieved name based on the search prefix.

    Returns None if the name does not start with the prefix.
    """
    if prefix is not None:
        # Trim back 'prefix' as needed and append the file name.
        # If 'prefix' does not end with '/' it will get trimmed back to the
        # parent of the last component.
        parent_dir = pp.dirname(prefix)
        if parent_dir:
            name = pp.join(parent_dir, name)

        # Check for prefix match
        if not name.startswith(prefix):
            return None

    return pp.normpath(name).lstrip("/")


def _modify_time_to_datetime(modify_time: str) -> datetime:
    # MLSD returns format: YYYYMMDDHHMMSS or YYYYMMDDHHMMSS.sss
    modify_str = modify_time.split(".")[0]  # Remove fractional seconds
    return datetime.strptime(modify_str, "%Y%m%d%H%M%S")
