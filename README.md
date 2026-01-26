blockcopy
=========

Copy large files (VM devices, LVM snapshots...) efficiently over network.

Designed for copying from/to NVMe disks over gigabit network.
Uses threadpool for computing hashes so that copy speed is not limited by CPU.


Installation
------------

You can just [download](https://raw.githubusercontent.com/messa/blockcopy/v0.0.2/blockcopy.py) the file `blockcopy.py` to `/usr/local/bin` and run it.

```shell
curl -fsSL https://raw.githubusercontent.com/messa/blockcopy/v0.0.2/blockcopy.py \
  -o /usr/local/bin/blockcopy
chmod +x /usr/local/bin/blockcopy
```

Or you can install this package using `pip install` and then run the script as `blockcopy`:

```shell
python3 -m pip install https://github.com/messa/blockcopy/archive/refs/tags/v0.0.2.zip
```


Usage
-----

```shell
blockcopy checksum /dev/destination \
  | ssh srchost blockcopy retrieve /dev/source \
  | blockcopy save /dev/destination
```

Or:

```shell
ssh dsthost blockcopy checksum /dev/destination \
  | blockcopy retrieve /dev/source \
  | ssh dsthost blockcopy save /dev/destination
```

### Options

**checksum:**
- `--progress` - show progress info
- `--start OFFSET` - start reading from this byte offset
- `--end OFFSET` - stop reading at this byte offset

**retrieve:**
- `--lzma` - compress blocks using LZMA before sending (useful for slow networks)

**save:**
- `--truncate` - truncate destination file to match source file size
- `-t, --times` - preserve timestamps (atime, mtime) from source file
- `-p, --perms` - preserve permissions from source file
- `-o, --owner` - preserve owner from source file
- `-g, --group` - preserve group from source file
- `--numeric-ids` - use numeric uid/gid instead of looking up user/group names


How it works
------------

The tool uses a three-stage pipeline connected via stdin/stdout:

1. **checksum** - Reads the destination file/device, splits it into 128 KB blocks, computes SHA3-512 hash for each block, and outputs the hashes as a binary stream.

2. **retrieve** - Reads hashes from stdin while simultaneously reading the source file. For each block, it compares the destination hash with the source block's hash. Only blocks that differ are sent to the output stream.

3. **save** - Reads block data from stdin and writes them to the destination file/device at the correct positions.

Each stage uses a ThreadPoolExecutor with multiple worker threads for hash computation, so the copy speed is not limited by single-core CPU performance. This is especially important for fast NVMe disks where sequential read speed can exceed what a single CPU core can hash.


Alternative software
--------------------

- [rsync](https://rsync.samba.org/)

  - Some versions of rsync do not support syncing block device contents.
  - The rolling hash algorithm can become too slow on large files (or large block devices).
    I've experienced slowdowns to 8-15 MB/s when 100 MB/s bandwidth was available.

- https://github.com/bscp-tool/bscp/blob/master/bscp

  - Slow hash computing (no threadpool)

- https://github.com/theraser/blocksync

  - Slow hash computing (no threadpool)

Internet discussions I found relevant to the topic of copying block devices over network:

- https://unix.stackexchange.com/questions/344756/is-there-anything-similar-to-rsync-to-syncing-block-devices

- https://www.reddit.com/r/linuxquestions/comments/desw0v/can_you_rsync_with_block_device_devsdx/
