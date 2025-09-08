#!/usr/bin/env python3

'''
Convert the checksum file to a human readable text file.

For debugging purposes.

Usage:
    checksum_to_text.py $checksum_file

Or:
    pv $checksum_file | checksum_to_text.py -
'''

from argparse import ArgumentParser
from contextlib import ExitStack


def main():
    p = ArgumentParser()
    p.add_argument('checksum_file')
    args = p.parse_args()

    with ExitStack() as stack:
        if args.checksum_file == '-':
            f = sys.stdin.buffer
        else:
            f = stack.enter_context(open(args.checksum_file, 'rb'))

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
                print(f'pos={block_pos} size={block_size} hash={block_hash}')
                del block_pos, block_size, block_hash
            elif command == b'hash':
                block_size = f.read(4)
                block_size = int.from_bytes(block_size, 'big')
                block_hash = f.read(64)
                block_hash = block_hash.hex()
                print(f'size={block_size} hash={block_hash}')
                del block_size, block_hash
            elif command == b'rest':
                block_size = f.read(8)
                block_size = int.from_bytes(block_size, 'big')
                print(f'rest size={block_size}')
                del block_size
            elif command == b'done':
                break
            else:
                raise Exception(f'Unknown command: {command}')


if __name__ == '__main__':
    main()
