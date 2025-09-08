#!/usr/bin/env python3

'''
Convert the checksum file to a human readable text file.

For debugging purposes.

For example when RAID 1 mirrors are out of sync and checksum of the whole
disk/volume comes out different each time, you can use this script to see
which blocks differ.

Usage:

    ./blockcopy.py checksum $file | ./checksum_to_text.py -
    # or
    ./blockcopy.py checksum $file > $checksum_file
    ./checksum_to_text.py $checksum_file
'''

from argparse import ArgumentParser
from contextlib import ExitStack
from sys import stdin


def main():
    p = ArgumentParser()
    p.add_argument('checksum_file')
    args = p.parse_args()

    with ExitStack() as stack:
        if args.checksum_file == '-':
            f = stdin.buffer
        else:
            f = stack.enter_context(open(args.checksum_file, 'rb'))

        computed_pos = 0
        while True:
            command = f.read(4)
            if not command:
                break
            if command == b'Hash':
                block_pos = f.read(8)
                block_pos = int.from_bytes(block_pos, 'big')
                block_size = f.read(4)
                block_size = int.from_bytes(block_size, 'big')
                block_hash = f.read(64)
                block_hash = block_hash.hex()
                print(f'pos={block_pos:014} size={block_size} hash={block_hash}', flush=True)
                if computed_pos != 0 and block_pos != computed_pos:
                    raise Exception(f'block_pos {block_pos} != computed_pos {computed_pos}')
                computed_pos = block_pos + block_size
                del block_pos, block_size, block_hash
            elif command == b'hash':
                block_size = f.read(4)
                block_size = int.from_bytes(block_size, 'big')
                block_hash = f.read(64)
                block_hash = block_hash.hex()
                print(f'pos={computed_pos:014} size={block_size} hash={block_hash}', flush=True)
                computed_pos += block_size
                del block_size, block_hash
            elif command == b'rest':
                block_pos = f.read(8)
                block_pos = int.from_bytes(block_pos, 'big')
                print(f'pos={block_pos:014} rest', flush=True)
                if block_pos != computed_pos:
                    raise Exception(f'block_pos {block_pos} != computed_pos {computed_pos}')
                computed_pos = block_pos
                del block_pos
            elif command == b'done':
                print(f'pos={computed_pos:014} done', flush=True)
                break
            else:
                raise Exception(f'Unknown command: {command}')


if __name__ == '__main__':
    main()
