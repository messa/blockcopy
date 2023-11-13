blockcopy
=========

Copy large files (VM devices, LVM snapshots...) effeciently over network.

Designed for copying from/to NVMe disks over gigabit network.
Uses threadpool for computing hashes so that copy speed is not limited by CPU.


Usage
-----

```shell
blockcopy.py checksum /dev/destination | ssh srchost blockcopy.py retrieve /dev/source | blockcopy.py save /dev/destination
```

Or:

```shell
ssh dsthost blockcopy.py checksum /dev/destination | blockcopy.py retrieve /dev/source | ssh dsthost blockcopy.py save /dev/destination
```


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
