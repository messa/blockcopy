# Plán: Přidat volby pro zachování metadata k příkazu save

## Přehled

Přidat volby `--times` (`-t`), `--perms` (`-p`), `--owner` (`-o`), `--group` (`-g`) k příkazu `save`, které způsobí nastavení příslušných metadata cílového souboru na hodnoty zdrojového souboru.

## Řešení

- **Retrieve** vždy odesílá metadata zdrojového souboru jako nový příkaz `Meta` v binárním protokolu
- **Save** aplikuje metadata pouze pro volby, které uživatel specifikoval

## Změny v `blockcopy.py`

### 1. Import (řádek ~41)

Přidat `chmod, chown, utime` k existujícímu importu z `os`:
```python
from os import chmod, chown, cpu_count, environ, getpid, kill, SEEK_END, utime
```

### 2. Argumenty pro save (řádek ~124)

```python
p_save.add_argument('--truncate', action='store_true', ...)
p_save.add_argument('--times', '-t', action='store_true', help='preserve timestamps from source file')
p_save.add_argument('--perms', '-p', action='store_true', help='preserve permissions from source file')
p_save.add_argument('--owner', '-o', action='store_true', help='preserve owner from source file')
p_save.add_argument('--group', '-g', action='store_true', help='preserve group from source file')
```

### 3. Volání do_save (řádek ~139)

```python
do_save(args.destination_file, stdin.buffer, truncate=args.truncate,
        preserve_times=args.times, preserve_perms=args.perms,
        preserve_owner=args.owner, preserve_group=args.group)
```

### 4. Rozšíření do_retrieve - odeslání Meta (před řádek ~695)

Po dokončení všech bloků, před odesláním `Done`, přidat:

```python
# Send metadata
with open(file_path, 'rb') as f:
    stat_result = os.stat(f.fileno())

with block_output_stream_lock:
    block_output_stream.write(b'Meta')
    # atime_ns (8 bytes) + mtime_ns (8 bytes) + mode (4 bytes) + uid (4 bytes) + gid (4 bytes) = 28 bytes
    block_output_stream.write(stat_result.st_atime_ns.to_bytes(8, 'big', signed=True))
    block_output_stream.write(stat_result.st_mtime_ns.to_bytes(8, 'big', signed=True))
    block_output_stream.write(stat_result.st_mode.to_bytes(4, 'big'))
    block_output_stream.write(stat_result.st_uid.to_bytes(4, 'big'))
    block_output_stream.write(stat_result.st_gid.to_bytes(4, 'big'))
    block_output_stream.flush()
```

### 5. Rozšíření do_save - signatura (řádek ~701)

```python
def do_save(file_path, block_input_stream, truncate=False,
            preserve_times=False, preserve_perms=False,
            preserve_owner=False, preserve_group=False):
```

### 6. Rozšíření do_save - zpracování Meta (v while loop, ~řádek 737)

Přidat novou větev pro příkaz `Meta`:

```python
elif command == b'Meta':
    # Read metadata: atime_ns(8) + mtime_ns(8) + mode(4) + uid(4) + gid(4) = 28 bytes
    meta_data = block_input_stream.read(28)
    if len(meta_data) != 28:
        raise IncompleteReadError('Incomplete read of metadata from block input stream')
    received_atime_ns = int.from_bytes(meta_data[0:8], 'big', signed=True)
    received_mtime_ns = int.from_bytes(meta_data[8:16], 'big', signed=True)
    received_mode = int.from_bytes(meta_data[16:20], 'big')
    received_uid = int.from_bytes(meta_data[20:24], 'big')
    received_gid = int.from_bytes(meta_data[24:28], 'big')
```

### 7. Rozšíření do_save - aplikace metadata (po ukončení loop)

Po úspěšném dokončení smyčky (za blokem `with open(...)`), aplikovat metadata:

```python
# Apply metadata after file is closed
if preserve_perms:
    chmod(file_path, received_mode)

if preserve_owner or preserve_group:
    uid = received_uid if preserve_owner else -1
    gid = received_gid if preserve_group else -1
    try:
        chown(file_path, uid, gid)
    except PermissionError as e:
        logger.warning('Failed to change owner/group: %s', e)

if preserve_times:
    utime(file_path, ns=(received_atime_ns, received_mtime_ns))
```

Poznámka: `utime` se aplikuje poslední, aby ostatní operace nepřepsaly mtime.

## Změny v `tests/test_copy.py`

Přidat nový test s parametrizací pro kombinace metadata voleb:

```python
@mark.parametrize('use_times', [False, True], ids=['no_times', 'times'])
@mark.parametrize('use_perms', [False, True], ids=['no_perms', 'perms'])
def test_copy_with_metadata(tmp_path, script_path, use_times, use_perms):
    # Test that metadata is correctly preserved when options are specified
```

## Binární protokol - nový příkaz Meta

Formát:
```
Meta (4 bytes "Meta")
atime_ns (8 bytes, signed big-endian)
mtime_ns (8 bytes, signed big-endian)
mode (4 bytes, unsigned big-endian)
uid (4 bytes, unsigned big-endian)
gid (4 bytes, unsigned big-endian)
```

Celkem: 4 + 28 = 32 bajtů

Pořadí příkazů v protokolu: `data/dlzm...` → `Meta` → `Done`

## Kritické soubory

- `/home/messa/code/gh/blockcopy/blockcopy.py` - hlavní změny
- `/home/messa/code/gh/blockcopy/tests/test_copy.py` - nové testy

## Ověření

1. Spustit existující testy: `pytest tests/`
2. Spustit nové testy pro metadata
3. Manuální test: kopírování souboru s různými kombinacemi voleb a ověření metadata pomocí `stat`
