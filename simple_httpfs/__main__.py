import argparse
import logging
import os.path as op
import sys
from typing import Any

from fuse import FUSE

from .httpfs import HttpFs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="""usage: simple-httpfs [OPTIONS] <mountpoint>"""
    )

    parser.add_argument("mountpoint")

    parser.add_argument(
        "-f",
        "--foreground",
        action="store_true",
        default=False,
        help="Run in the foreground",
    )

    parser.add_argument("--sentinel", default="...")

    parser.add_argument("--block-size", default=2**20, type=int)

    parser.add_argument("--lru-capacity", default=400, type=int)

    parser.add_argument("--disk-cache-size", default=2**30, type=int)

    parser.add_argument("--disk-cache-dir", default="/tmp/xx")

    parser.add_argument(
        "--allow-other",
        action="store_true",
        default=False,
        help="Allow other users to access this fuse",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable debug logging",
    )

    parser.add_argument("-l", "--log", default=None, type=str)

    args = vars(parser.parse_args())

    if not op.isdir(args["mountpoint"]):
        print(
            "Mount point must be a directory: {}".format(args["mountpoint"]),
            file=sys.stderr,
        )
        sys.exit(1)

    logger = logging.getLogger("simple-httpfs")
    if args["log"]:
        handler = logging.FileHandler(args["log"])
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(module)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        logging.basicConfig(level=logging.INFO)

    if args["verbose"]:
        logger.setLevel(logging.DEBUG)

    platform_settings: dict[str, Any] = {}
    if sys.platform == "darwin":
        platform_settings["noapplexattr"] = True
        platform_settings["noappledouble"] = True

    start_msg = f"""
Mounting HTTP Filesystem...
    mountpoint: {args["mountpoint"]}
    foreground: {args["foreground"]}
    allow others: {args["allow_other"]}
    direct_io: True
"""
    print(start_msg, file=sys.stderr)

    fs = HttpFs(
        sentinel=args["sentinel"],
        disk_cache_size=args["disk_cache_size"],
        disk_cache_dir=args["disk_cache_dir"],
        lru_capacity=args["lru_capacity"],
        block_size=args["block_size"],
        logger=logger,
    )

    _ = FUSE(
        fs,
        args["mountpoint"],
        foreground=args["foreground"],
        allow_other=args["allow_other"],
        direct_io=True,
        **platform_settings,
    )


if __name__ == "__main__":
    main()
