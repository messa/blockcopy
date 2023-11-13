blockcopy
=========

Copy large files (VM devices, LVM snapshots...) effeciently over network.


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

- https://github.com/bscp-tool/bscp/blob/master/bscp

- https://github.com/theraser/blocksync

- https://unix.stackexchange.com/questions/344756/is-there-anything-similar-to-rsync-to-syncing-block-devices

- https://www.reddit.com/r/linuxquestions/comments/desw0v/can_you_rsync_with_block_device_devsdx/
