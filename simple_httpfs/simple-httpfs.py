#!/usr/bin/env python

import logging
import os.path as op
from fuse import FUSE

from simple_httpfs import HttpFs

def main():
    import argparse
    parser = argparse.ArgumentParser(description="""
    usage: simple-httpfs <mountpoint>
""")
    parser.add_argument('mountpoint')
    parser.add_argument(
        '-f', '--foreground',
        action='store_true',
        default=False,
    	help='Run in the foreground')

    parser.add_argument(
        '--schema', default=None, type=str)
    parser.add_argument(
        '--disk-cache-size', default=2**30, type=int)
    parser.add_argument(
        '--disk-cache-dir', default='/tmp/xx')
    parser.add_argument(
        '--lru-capacity', default=400, type=int)

    args = vars(parser.parse_args())

    logging.getLogger().setLevel(logging.INFO)
    logging.info("starting:")
    logging.info("foreground: {}".format(args['foreground']))

    if op.isfile(args['mountpoint']):
        print("Mount point must be a directory:", args['mountpoint'],
                file=sys.stderr)
        return

    schema = op.split(args['mountpoint'])[1]
    print("schema:", schema)

    if schema not in ['http', 'https', 'ftp']:
        if args['schema'] is None:
            print('Could not infer schema. Try specifying either http, https or ftp ' +
                    'using the --schema argument')
            return
        if args['schema'] not in ['http', 'https', 'ftp']:
            print('Specified schema ({}) not one of http, https or ftp'.format(schema))
            return

    fuse = FUSE(
        HttpFs(
               schema,
               disk_cache_size=args['disk_cache_size'],
               disk_cache_dir=args['disk_cache_dir'],
               lru_capacity=args['lru_capacity']
            ),
        args['mountpoint'],
        foreground=args['foreground']
    )


if __name__ == '__main__':
    main()
