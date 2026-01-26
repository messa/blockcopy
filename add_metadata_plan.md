# Plán: Přidat volby pro zachování metadata k příkazu save


## Přehled

Přidat volby `--times` (`-t`), `--perms` (`-p`), `--owner` (`-o`), `--group` (`-g`) k příkazu `save`, které způsobí nastavení příslušných metadata cílového souboru na hodnoty zdrojového souboru.

## Řešení

- **Retrieve** vždy odesílá metadata zdrojového souboru jako nový příkaz `Meta` v binárním protokolu
- **Save** aplikuje metadata pouze pro volby, které uživatel specifikoval

## Kompatibilita s rsync

Chování voleb `--owner`, `--group` a `--numeric-ids` je konzistentní s rsync:

| Scénář | rsync | blockcopy |
|--------|-------|-----------|
| Bez `--numeric-ids` | Přenese jméno, hledá podle jména, fallback na UID/GID | Stejné |
| S `--numeric-ids` | Použije přímo UID/GID | Stejné |
| Uživatel/skupina neexistuje | Fallback na původní UID/GID | Stejné (s warningem) |

Rozdíl v implementaci: rsync posílá jména pouze bez `--numeric-ids`, zatímco blockcopy posílá vždy obojí (jména i ID) a rozhodnutí se dělá až na straně save. To je flexibilnější - retrieve nemusí vědět, jaké volby uživatel použije na straně save.

## Změny v `blockcopy.py`

### 1. Importy (řádek ~41)

Přidat `chmod, chown, utime` k existujícímu importu z `os`:
```python
from os import chmod, chown, cpu_count, environ, getpid, kill, SEEK_END, utime
```

Přidat import pro lookup uživatelů a skupin (standardní knihovna, pouze Unix):
```python
from pwd import getpwuid, getpwnam
from grp import getgrgid, getgrnam
```

Moduly `pwd` a `grp` jsou součástí standardní knihovny Pythonu (dostupné pouze na Unix-like systémech):
- `getpwuid(uid)` - vrátí záznam z passwd databáze podle UID, obsahuje `.pw_name` (jméno uživatele)
- `getpwnam(name)` - vrátí záznam z passwd databáze podle jména, obsahuje `.pw_uid` (UID)
- `getgrgid(gid)` - vrátí záznam z group databáze podle GID, obsahuje `.gr_name` (jméno skupiny)
- `getgrnam(name)` - vrátí záznam z group databáze podle jména, obsahuje `.gr_gid` (GID)

Všechny funkce vyhodí `KeyError` pokud uživatel/skupina neexistuje.

### 2. Argumenty pro save (řádek ~124)

```python
p_save.add_argument('--truncate', action='store_true', ...)
p_save.add_argument('--times', '-t', action='store_true', help='preserve timestamps from source file')
p_save.add_argument('--perms', '-p', action='store_true', help='preserve permissions from source file')
p_save.add_argument('--owner', '-o', action='store_true', help='preserve owner from source file')
p_save.add_argument('--group', '-g', action='store_true', help='preserve group from source file')
p_save.add_argument('--numeric-ids', action='store_true', help='use numeric uid/gid instead of looking up user/group names')
```

### 3. Volání do_save (řádek ~139)

```python
do_save(args.destination_file, stdin.buffer, truncate=args.truncate,
        preserve_times=args.times, preserve_perms=args.perms,
        preserve_owner=args.owner, preserve_group=args.group,
        numeric_ids=args.numeric_ids)
```

### 4. Rozšíření do_retrieve - odeslání Meta (před řádek ~695)

Po dokončení všech bloků, před odesláním `Done`, přidat:

```python
# Send metadata
with open(file_path, 'rb') as f:
    stat_result = os.stat(f.fileno())

# Get owner/group names (may fail if uid/gid doesn't exist)
try:
    owner_name = getpwuid(stat_result.st_uid).pw_name.encode('utf-8')
except KeyError:
    owner_name = b''
try:
    group_name = getgrgid(stat_result.st_gid).gr_name.encode('utf-8')
except KeyError:
    group_name = b''

with block_output_stream_lock:
    block_output_stream.write(b'Meta')
    # atime_ns (8 bytes) + mtime_ns (8 bytes) + mode (4 bytes) + uid (4 bytes) + gid (4 bytes)
    # + owner_name_len (2 bytes) + owner_name + group_name_len (2 bytes) + group_name
    block_output_stream.write(stat_result.st_atime_ns.to_bytes(8, 'big', signed=True))
    block_output_stream.write(stat_result.st_mtime_ns.to_bytes(8, 'big', signed=True))
    block_output_stream.write(stat_result.st_mode.to_bytes(4, 'big'))
    block_output_stream.write(stat_result.st_uid.to_bytes(4, 'big'))
    block_output_stream.write(stat_result.st_gid.to_bytes(4, 'big'))
    block_output_stream.write(len(owner_name).to_bytes(2, 'big'))
    block_output_stream.write(owner_name)
    block_output_stream.write(len(group_name).to_bytes(2, 'big'))
    block_output_stream.write(group_name)
    block_output_stream.flush()
```

### 5. Rozšíření do_save - signatura (řádek ~701)

```python
def do_save(file_path, block_input_stream, truncate=False,
            preserve_times=False, preserve_perms=False,
            preserve_owner=False, preserve_group=False,
            numeric_ids=False):
```

### 6. Rozšíření do_save - zpracování Meta (v while loop, ~řádek 737)

Přidat novou větev pro příkaz `Meta`:

```python
elif command == b'meta':
    # Read metadata: atime_ns(8) + mtime_ns(8) + mode(4) + uid(4) + gid(4) = 28 bytes
    meta_data = block_input_stream.read(28)
    if len(meta_data) != 28:
        raise IncompleteReadError('Incomplete read of metadata from block input stream')
    received_atime_ns = int.from_bytes(meta_data[0:8], 'big', signed=True)
    received_mtime_ns = int.from_bytes(meta_data[8:16], 'big', signed=True)
    received_mode = int.from_bytes(meta_data[16:20], 'big')
    received_uid = int.from_bytes(meta_data[20:24], 'big')
    received_gid = int.from_bytes(meta_data[24:28], 'big')

    # Read owner name
    owner_name_len_bytes = block_input_stream.read(2)
    if len(owner_name_len_bytes) != 2:
        raise IncompleteReadError('Incomplete read of owner name length')
    owner_name_len = int.from_bytes(owner_name_len_bytes, 'big')
    received_owner_name = block_input_stream.read(owner_name_len).decode('utf-8') if owner_name_len > 0 else ''

    # Read group name
    group_name_len_bytes = block_input_stream.read(2)
    if len(group_name_len_bytes) != 2:
        raise IncompleteReadError('Incomplete read of group name length')
    group_name_len = int.from_bytes(group_name_len_bytes, 'big')
    received_group_name = block_input_stream.read(group_name_len).decode('utf-8') if group_name_len > 0 else ''
```

### 7. Rozšíření do_save - aplikace metadata (po ukončení loop)

Po úspěšném dokončení smyčky (za blokem `with open(...)`), aplikovat metadata:

```python
# Apply metadata after file is closed
if preserve_perms:
    chmod(file_path, received_mode)

if preserve_owner or preserve_group:
    uid = -1
    gid = -1

    if preserve_owner:
        if numeric_ids or not received_owner_name:
            uid = received_uid
        else:
            try:
                uid = getpwnam(received_owner_name).pw_uid
            except KeyError:
                logger.warning('User %r not found, falling back to uid %d', received_owner_name, received_uid)
                uid = received_uid

    if preserve_group:
        if numeric_ids or not received_group_name:
            gid = received_gid
        else:
            try:
                gid = getgrnam(received_group_name).gr_gid
            except KeyError:
                logger.warning('Group %r not found, falling back to gid %d', received_group_name, received_gid)
                gid = received_gid

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
meta (4 bytes "Meta")
atime_ns (8 bytes, signed big-endian)
mtime_ns (8 bytes, signed big-endian)
mode (4 bytes, unsigned big-endian)
uid (4 bytes, unsigned big-endian)
gid (4 bytes, unsigned big-endian)
owner_name_len (2 bytes, unsigned big-endian)
owner_name (owner_name_len bytes, UTF-8)
group_name_len (2 bytes, unsigned big-endian)
group_name (group_name_len bytes, UTF-8)
```

Fixní část: 4 + 8 + 8 + 4 + 4 + 4 + 2 + 2 = 36 bajtů + délka jmen

Pořadí příkazů v protokolu: `data/dlzm...` → `meta` → `Done`

## Kritické soubory

- `/home/messa/code/gh/blockcopy/blockcopy.py` - hlavní změny
- `/home/messa/code/gh/blockcopy/tests/test_copy.py` - nové testy

## Ověření

1. Spustit existující testy: `pytest tests/`
2. Spustit nové testy pro metadata
3. Manuální test: kopírování souboru s různými kombinacemi voleb a ověření metadata pomocí `stat`
