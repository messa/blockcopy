# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

blockcopy is a Python CLI tool for efficiently copying large files and block devices (VM devices, LVM snapshots) over network. It uses a three-stage pipeline with hash comparison to transfer only changed blocks, making incremental copies fast. Hash computation is parallelized via ThreadPoolExecutor to avoid CPU bottlenecks on fast NVMe disks.

## Commands

### Run tests
```bash
make check                    # Run all tests with uv
uv run pytest -s -vv tests    # Same as above, explicit
uv run pytest tests/test_checksum.py::test_checksum_file  # Run single test
```

### Lint
```bash
make lint        # Run flake8
uv run flake8
```

### Install for development
```bash
uv sync          # Install dependencies
```

## Architecture

The tool is a single-file script (`blockcopy.py`) with three subcommands that form a pipeline connected via stdin/stdout:

```
blockcopy checksum /dev/destination | \
  ssh srchost blockcopy retrieve /dev/source | \
  blockcopy save /dev/destination
```

### Pipeline stages

1. **checksum** - Reads destination file/device, computes SHA3-512 hash for each 128KB block, outputs binary hash stream
2. **retrieve** - Reads source file, compares hashes from stdin, outputs only differing blocks as binary data stream
3. **save** - Reads block data from stdin, writes to destination file/device

### Concurrency model

Each stage uses `ThreadPoolExecutor` with:
- 1 read worker thread (reads file sequentially)
- N hash worker threads (compute SHA3-512 in parallel, N = min(cpu_count, 8))
- 1 send worker thread (writes output stream)

Threads communicate via `Queue` objects. `ExceptionCollector` aggregates errors from worker threads.

### Binary protocol

- `Hash` command: 4 bytes cmd + 8 bytes position + 4 bytes size + 64 bytes SHA3-512 digest
- `data` command: 4 bytes cmd + 8 bytes position + 4 bytes size + N bytes data
- `dlzm` command: same as `data` but LZMA compressed (optional `--lzma` flag)
- `rest` command: signals to send remaining data beyond checksummed range
- `Done` command: 4 bytes cmd + 8 bytes total size (signals completion)

## Key constants

- Block size: 128 KB (`block_size = 128 * 1024`)
- Hash algorithm: SHA3-512 (`sha3_512`)
- Worker threads: min(cpu_count, 8)

## Versioning

When releasing a new version, update the version string in these locations:

1. `blockcopy.py` - `__version__` variable
2. `pyproject.toml` - `version` field in `[project]` section
3. `README.md` - download URLs contain the version tag (e.g. `v0.0.2`)
