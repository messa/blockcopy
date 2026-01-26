# Plán: Přidat volby pro zachování metadata k příkazu save

## Cíl

**Hlavní cíl:** Po synchronizaci souborů pomocí blockcopy by rsync měl považovat soubory za shodné a nezpracovávat je znovu.

To umožní workflow, kde se blockcopy použije pro efektivní přenos velkých souborů (využívá blokový delta-transfer), a následný rsync pouze ověří, že soubory jsou shodné, aniž by je znovu přenášel.

## Analýza: Jak rsync rozhoduje o přenosu souborů

Rsync používá **"quick check" algoritmus** (viz `generator.c:617-671` ve zdrojovém kódu rsync):

### Rozhodovací logika pro běžné soubory

1. **Velikost** - pokud se liší `st_size`, soubor se přenese
2. Pokud `--checksum`: porovná se checksum celého souboru (pomalé)
3. Pokud `--size-only`: po ověření velikosti se soubor považuje za shodný
4. Pokud `--ignore-times`: soubor se vždy přenese (ignoruje mtime)
5. **Modification time (mtime)** - pokud se liší, soubor se přenese

### Porovnání mtime (viz `util1.c:1408-1418`)

```c
int same_time(time_t f1_sec, unsigned long f1_nsec, time_t f2_sec, unsigned long f2_nsec)
{
    if (modify_window == 0)
        return f1_sec == f2_sec;           // Výchozí: pouze sekundy!
    if (modify_window < 0)
        return f1_sec == f2_sec && f1_nsec == f2_nsec;  // I nanosekundy
    // Kladná hodnota: toleruje rozdíl v sekundách
    ...
}
```

**Klíčový poznatek:** Ve výchozím nastavení (`modify_window == 0`) rsync porovnává mtime **pouze na úrovni sekund**. Nanosekundy se použijí pouze s `--modify-window=-1`.

### Co rsync kontroluje při rozhodování o přenosu obsahu

Rsync při "quick check" kontroluje **pouze**:
- **Velikost souboru** (`st_size`)
- **Modification time** (`st_mtime`) - ve výchozím nastavení jen sekundy

**Permissions, owner a group NEOVLIVŇUJÍ rozhodnutí o přenosu obsahu!** Pokud se liší, rsync je pouze aktualizuje pomocí `set_file_attrs()`, ale obsah souboru nepřenáší (viz `generator.c:1809-1814`).

## Vyhodnocení: Splní tento plán cíl?

**ANO**, plánovaná úprava splní cíl, pokud:

1. **`--truncate`** se použije při save - zajistí shodnou velikost souboru
2. **`--times`** se použije při save - zajistí shodný mtime (sekundy budou shodné)

Volby `--perms`, `--owner`, `--group` jsou **volitelné** pro rsync kompatibilitu - pokud se nepoužijí, rsync pouze aktualizuje atributy bez přenosu obsahu (rychlé)

### Doporučené použití pro rsync kompatibilitu

```bash
# Minimální konfigurace pro rsync kompatibilitu (bez přenosu obsahu)
blockcopy.py checksum /dest | ssh src blockcopy.py retrieve /src | \
    blockcopy.py save --truncate --times /dest

# Následný rsync nepřenese žádný obsah (pouze případně aktualizuje atributy)
rsync -av /src/ /dest/

# Pro úplnou shodu včetně atributů (rsync nic nezmění)
blockcopy.py checksum /dest | ssh src blockcopy.py retrieve /src | \
    blockcopy.py save --truncate --times --perms --owner --group /dest
```

### Poznámky k přesnosti času

- blockcopy přenáší mtime s nanosekundovou přesností (`st_mtime_ns`)
- rsync ve výchozím nastavení porovnává pouze sekundy
- Sekundová složka bude po `os.utime(ns=...)` shodná → rsync bude spokojený

## Přehled implementace

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
owner_name = get_user_name_by_uid(stat_result.st_uid).encode('utf-8')
group_name = get_group_name_by_gid(stat_result.st_gid).encode('utf-8')

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

## Override metadata přes env proměnné (pro testy)

V `do_retrieve` přidat podporu pro override hodnot přes env proměnné. To umožní v testech nastavit pevné hodnoty a zachovat stávající přesné byte asserty.

```python
# V do_retrieve, po získání stat_result:
atime_ns = int(environ.get('BLOCKCOPY_OVERRIDE_ATIME_NS', stat_result.st_atime_ns))
mtime_ns = int(environ.get('BLOCKCOPY_OVERRIDE_MTIME_NS', stat_result.st_mtime_ns))
mode = int(environ.get('BLOCKCOPY_OVERRIDE_MODE', stat_result.st_mode))
uid = int(environ.get('BLOCKCOPY_OVERRIDE_UID', stat_result.st_uid))
gid = int(environ.get('BLOCKCOPY_OVERRIDE_GID', stat_result.st_gid))
owner_name = environ.get('BLOCKCOPY_OVERRIDE_OWNER', get_user_name_by_uid(stat_result.st_uid))
group_name = environ.get('BLOCKCOPY_OVERRIDE_GROUP', get_group_name_by_gid(stat_result.st_gid))
```

```python
def get_user_name_by_uid(uid):
    try:
        return getpwuid(uid).pw_name
    except KeyError:
        return ''

def get_group_name_by_gid(gid):
    try:
        return getgrgid(gid).gr_name
    except KeyError:
        return ''
```

## Změny v `tests/test_copy.py`

### Úprava existujících testů

Testy s přesnými byte asserty (např. `test_copy_tiny`) přidat env override, aby výstup byl deterministický:

```python
def test_copy_tiny(tmp_path, script_path):
    # ... setup ...

    env = {
        **environ,
        'BLOCKCOPY_OVERRIDE_ATIME_NS': '0',
        'BLOCKCOPY_OVERRIDE_MTIME_NS': '0',
        'BLOCKCOPY_OVERRIDE_MODE': '33188',  # 0o100644
        'BLOCKCOPY_OVERRIDE_UID': '1000',
        'BLOCKCOPY_OVERRIDE_GID': '1000',
        'BLOCKCOPY_OVERRIDE_OWNER': 'testuser',
        'BLOCKCOPY_OVERRIDE_GROUP': 'testgroup',
    }

    # ... Popen calls with env=env ...

    # Přesný byte assert nyní funguje, protože Meta má deterministické hodnoty
    assert p2_output == b'data...' + expected_meta_bytes + b'Done...'
```

### Nové testy pro metadata

```python
@mark.parametrize('use_times', [False, True], ids=['no_times', 'times'])
@mark.parametrize('use_perms', [False, True], ids=['no_perms', 'perms'])
def test_copy_with_metadata(tmp_path, script_path, use_times, use_perms):
    '''Test that metadata options correctly preserve file attributes.'''
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)

    # Set specific metadata on source file
    chmod(src_path, 0o640)
    utime(src_path, ns=(1000000000000, 2000000000000))

    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    src_stat = stat(src_path)
    dst_stat_before = stat(dst_path)

    cmd3_args = ['--times'] if use_times else []
    cmd3_args += ['--perms'] if use_perms else []

    # ... run pipeline ...

    dst_stat_after = stat(dst_path)

    if use_times:
        assert dst_stat_after.st_mtime_ns == src_stat.st_mtime_ns
    else:
        assert dst_stat_after.st_mtime_ns != src_stat.st_mtime_ns

    if use_perms:
        assert dst_stat_after.st_mode == src_stat.st_mode
```

### Testy pro rsync kompatibilitu

Nový soubor `tests/test_rsync_compat.py`:

```python
import shutil
import subprocess
from os import chmod, environ, stat, utime
from pathlib import Path

import pytest


# Skip tests if rsync is not available locally, but fail on CI
def _rsync_available():
    return shutil.which('rsync') is not None

def _running_on_ci():
    # Common CI environment variables
    return any(environ.get(var) for var in ['CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_URL'])

# On CI: fail if rsync missing. Locally: skip gracefully.
if not _rsync_available():
    if _running_on_ci():
        raise RuntimeError("rsync is required for CI but not found in PATH")
    pytestmark = pytest.mark.skip(reason="rsync not available")


def test_rsync_sees_no_changes_after_blockcopy(tmp_path, script_path):
    """
    After blockcopy sync with --truncate --times, rsync should see
    no differences and transfer nothing.
    """
    # Setup: create source file with specific content and metadata
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    src_file = src_dir / 'testfile.bin'

    test_content = b'Hello World! ' * 1000  # ~13KB
    src_file.write_bytes(test_content)

    # Set specific mtime (not "now" to avoid race conditions)
    # Use a time in the past with zero nanoseconds for clean comparison
    fixed_mtime_ns = 1700000000_000000000  # 2023-11-14 22:13:20 UTC
    utime(src_file, ns=(fixed_mtime_ns, fixed_mtime_ns))

    # Setup: create destination file (different content, will be overwritten)
    dst_dir = tmp_path / 'dst'
    dst_dir.mkdir()
    dst_file = dst_dir / 'testfile.bin'
    dst_file.write_bytes(b'X' * len(test_content))  # Same size, different content

    # Run blockcopy pipeline with --truncate --times
    p1 = subprocess.Popen(
        ['python3', script_path, 'checksum', str(dst_file)],
        stdout=subprocess.PIPE,
    )
    p2 = subprocess.Popen(
        ['python3', script_path, 'retrieve', str(src_file)],
        stdin=p1.stdout,
        stdout=subprocess.PIPE,
    )
    p1.stdout.close()
    p3 = subprocess.Popen(
        ['python3', script_path, 'save', '--truncate', '--times', str(dst_file)],
        stdin=p2.stdout,
        stdout=subprocess.PIPE,
    )
    p2.stdout.close()

    p3.communicate()
    assert p1.wait() == 0
    assert p2.wait() == 0
    assert p3.wait() == 0

    # Verify content was synced
    assert dst_file.read_bytes() == test_content

    # Verify mtime matches (at least seconds)
    src_stat = stat(src_file)
    dst_stat = stat(dst_file)
    assert int(src_stat.st_mtime) == int(dst_stat.st_mtime), \
        f"mtime mismatch: src={src_stat.st_mtime}, dst={dst_stat.st_mtime}"

    # NOW: Run rsync --dry-run --itemize-changes and verify NO transfers
    result = subprocess.run(
        [
            'rsync',
            '--dry-run',
            '--itemize-changes',  # Shows what would be transferred
            '-a',                 # Archive mode
            '--no-owner',         # Skip owner check (we didn't sync owner)
            '--no-group',         # Skip group check (we didn't sync group)
            '--no-perms',         # Skip perms check (we didn't sync perms)
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"rsync failed: {result.stderr}"

    # itemize-changes output should be empty if no transfer needed
    assert result.stdout.strip() == '', \
        f"rsync would transfer file! itemize output:\n{result.stdout}"


def test_rsync_sees_no_changes_with_full_metadata(tmp_path, script_path):
    """
    After blockcopy sync with all metadata options, rsync -a should see
    no differences at all.
    """
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    src_file = src_dir / 'testfile.bin'

    test_content = b'Test content for full metadata sync'
    src_file.write_bytes(test_content)

    # Set specific metadata
    chmod(src_file, 0o644)
    fixed_mtime_ns = 1700000000_000000000
    utime(src_file, ns=(fixed_mtime_ns, fixed_mtime_ns))

    dst_dir = tmp_path / 'dst'
    dst_dir.mkdir()
    dst_file = dst_dir / 'testfile.bin'
    dst_file.write_bytes(b'-' * len(test_content))

    # Run blockcopy with full metadata
    p1 = subprocess.Popen(
        ['python3', script_path, 'checksum', str(dst_file)],
        stdout=subprocess.PIPE,
    )
    p2 = subprocess.Popen(
        ['python3', script_path, 'retrieve', str(src_file)],
        stdin=p1.stdout,
        stdout=subprocess.PIPE,
    )
    p1.stdout.close()
    p3 = subprocess.Popen(
        ['python3', script_path, 'save',
         '--truncate', '--times', '--perms',
         str(dst_file)],
        stdin=p2.stdout,
        stdout=subprocess.PIPE,
    )
    p2.stdout.close()

    p3.communicate()
    assert p1.wait() == 0
    assert p2.wait() == 0
    assert p3.wait() == 0

    # Run rsync -a (without --no-owner/group since we're not root anyway)
    result = subprocess.run(
        [
            'rsync',
            '--dry-run',
            '--itemize-changes',
            '-a',
            '--no-owner',  # Can't test owner without root
            '--no-group',  # Can't test group without root
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == '', \
        f"rsync would make changes! itemize output:\n{result.stdout}"


def test_rsync_with_nanoseconds_sees_no_changes(tmp_path, script_path):
    """
    With rsync --modify-window=-1 (nanosecond comparison), blockcopy
    should still produce identical files because it preserves full
    nanosecond precision via os.utime(ns=...).
    """
    src_file = tmp_path / 'src.bin'
    dst_file = tmp_path / 'dst.bin'

    test_content = b'Nanosecond precision test'
    src_file.write_bytes(test_content)

    # Set mtime with non-zero nanoseconds
    mtime_ns = 1700000000_123456789  # Has nanosecond component
    utime(src_file, ns=(mtime_ns, mtime_ns))

    dst_file.write_bytes(b'X' * len(test_content))

    # Run blockcopy with --truncate --times
    p1 = subprocess.Popen(
        ['python3', script_path, 'checksum', str(dst_file)],
        stdout=subprocess.PIPE,
    )
    p2 = subprocess.Popen(
        ['python3', script_path, 'retrieve', str(src_file)],
        stdin=p1.stdout,
        stdout=subprocess.PIPE,
    )
    p1.stdout.close()
    p3 = subprocess.Popen(
        ['python3', script_path, 'save', '--truncate', '--times', str(dst_file)],
        stdin=p2.stdout,
        stdout=subprocess.PIPE,
    )
    p2.stdout.close()

    p3.communicate()
    assert p1.wait() == 0
    assert p2.wait() == 0
    assert p3.wait() == 0

    # Verify nanoseconds match exactly
    src_stat = stat(src_file)
    dst_stat = stat(dst_file)
    assert src_stat.st_mtime_ns == dst_stat.st_mtime_ns, \
        f"mtime_ns mismatch: src={src_stat.st_mtime_ns}, dst={dst_stat.st_mtime_ns}"

    # Run rsync with --modify-window=-1 (nanosecond comparison)
    result = subprocess.run(
        [
            'rsync',
            '--dry-run',
            '--itemize-changes',
            '-a',
            '--modify-window=-1',  # Compare nanoseconds too!
            '--no-owner',
            '--no-group',
            '--no-perms',
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"rsync failed: {result.stderr}"
    assert result.stdout.strip() == '', \
        f"rsync --modify-window=-1 would transfer! output:\n{result.stdout}"


def test_rsync_without_times_would_transfer(tmp_path, script_path):
    """
    Negative test: without --times, rsync SHOULD see differences
    (because mtime won't match).
    """
    src_file = tmp_path / 'src.bin'
    dst_file = tmp_path / 'dst.bin'

    test_content = b'Test'
    src_file.write_bytes(test_content)

    # Set old mtime on source
    old_mtime_ns = 1600000000_000000000
    utime(src_file, ns=(old_mtime_ns, old_mtime_ns))

    dst_file.write_bytes(b'X' * len(test_content))

    # Run blockcopy WITHOUT --times
    p1 = subprocess.Popen(
        ['python3', script_path, 'checksum', str(dst_file)],
        stdout=subprocess.PIPE,
    )
    p2 = subprocess.Popen(
        ['python3', script_path, 'retrieve', str(src_file)],
        stdin=p1.stdout,
        stdout=subprocess.PIPE,
    )
    p1.stdout.close()
    p3 = subprocess.Popen(
        ['python3', script_path, 'save', '--truncate', str(dst_file)],
        stdin=p2.stdout,
        stdout=subprocess.PIPE,
    )
    p2.stdout.close()

    p3.communicate()
    assert p1.wait() == 0
    assert p2.wait() == 0
    assert p3.wait() == 0

    # rsync SHOULD see difference (mtime differs)
    result = subprocess.run(
        ['rsync', '--dry-run', '--itemize-changes', '-a',
         '--no-owner', '--no-group', '--no-perms',
         str(src_file), str(dst_file)],
        capture_output=True,
        text=True,
    )

    # Expect non-empty output (rsync would transfer)
    assert result.stdout.strip() != '', \
        "rsync should see mtime difference without --times!"
```

**Vysvětlení testů:**

1. **`test_rsync_sees_no_changes_after_blockcopy`** - hlavní test: po blockcopy s `--truncate --times` rsync nevidí žádné změny
2. **`test_rsync_sees_no_changes_with_full_metadata`** - s plnými metadata (`--perms`) rsync také nic nemění
3. **`test_rsync_with_nanoseconds_sees_no_changes`** - test s `rsync --modify-window=-1` (nanosekundové porovnání) - ověřuje, že blockcopy zachovává plnou nanosekundovou přesnost
4. **`test_rsync_without_times_would_transfer`** - negativní test: BEZ `--times` rsync VIDÍ rozdíl (mtime se liší)

**Klíč k ověření:** `rsync --dry-run --itemize-changes` - prázdný výstup = soubory jsou shodné.

**Poznámka k nanosekudnám:** rsync od verze 3.1.3 podporuje `--modify-window=-1` pro porovnávání nanosekund. blockcopy používá `os.utime(ns=...)` který zachovává plnou přesnost, takže je kompatibilní i s tímto režimem.

**Poznámka k dostupnosti rsync:** Testy se přeskočí, pokud rsync není nainstalován (lokální vývoj). Na CI (detekce přes `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `JENKINS_URL` env vars) testy selžou s chybou, pokud rsync chybí - rsync musí být na CI nainstalován.

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
