# Plán implementace --recursive pro blockcopy

## Přehled

Tento dokument popisuje návrh implementace rekurzivní synchronizace adresářů pro nástroj blockcopy. Cílem je umožnit synchronizaci celých adresářových stromů podobně jako `rsync -r`.

## Současný stav

Blockcopy aktuálně podporuje pouze synchronizaci jednotlivých souborů nebo blokových zařízení. Pipeline vypadá takto:

```
blockcopy checksum /dev/destination | \
  ssh srchost blockcopy retrieve /dev/source | \
  blockcopy save /dev/destination
```

## Navrhované použití

```
blockcopy checksum --recursive /path/to/dst_dir | \
  ssh srchost blockcopy retrieve --recursive /path/to/src_dir | \
  blockcopy save --recursive /path/to/dst_dir
```

## Architektura řešení

### Fáze 1: Výměna seznamu souborů

Před samotným přenosem dat je nutné vyměnit informace o struktuře adresářů na obou stranách.

**Nový průběh:**
1. `checksum --recursive` projde cílový adresář a odešle seznam všech souborů s jejich metadaty
2. `retrieve --recursive` přijme seznam z cíle, projde zdrojový adresář a:
   - Porovná struktury
   - Rozhodne, které soubory je třeba synchronizovat
   - Pro každý soubor, který existuje na obou stranách, vyžádá checksums
3. `save --recursive` aplikuje změny

### Fáze 2: Přenos souborů

Pro každý soubor, který se liší, proběhne standardní checksum/retrieve/save cyklus.

---

## Úpravy binárního protokolu

### Nové příkazy checksum → retrieve

#### `list` - začátek výpisu souborů
```
4 bytes: příkaz "list"
8 bytes: počet položek (N)
```

#### `file` - soubor v adresáři
```
4 bytes: příkaz "file"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
8 bytes: velikost souboru
8 bytes: mtime_ns
4 bytes: mode
4 bytes: uid
4 bytes: gid
2 bytes: délka owner_name
N bytes: owner_name
2 bytes: délka group_name
N bytes: group_name
```

#### `dirs` - adresář
```
4 bytes: příkaz "dirs"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
8 bytes: mtime_ns
4 bytes: mode
4 bytes: uid
4 bytes: gid
2 bytes: délka owner_name
N bytes: owner_name
2 bytes: délka group_name
N bytes: group_name
```

#### `link` - symbolický odkaz
```
4 bytes: příkaz "link"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
2 bytes: délka cíle odkazu (UTF-8)
N bytes: cíl odkazu
8 bytes: mtime_ns
4 bytes: uid
4 bytes: gid
```

#### `ends` - konec výpisu seznamu
```
4 bytes: příkaz "ends"
```

#### `getc` - požadavek na checksums pro konkrétní soubor
```
4 bytes: příkaz "getc"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
```

Po `getc` následuje standardní sekvence `Hash` příkazů pro daný soubor, ukončená `done`.

### Nové příkazy retrieve → save

#### `file` - začátek dat pro soubor
```
4 bytes: příkaz "file"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
```

Po tomto následují standardní `data`/`dlzm` příkazy pro tento soubor.

#### `fend` - konec dat pro soubor
```
4 bytes: příkaz "fend"
```

Následuje `meta` příkaz s metadaty souboru.

#### `mkdr` - vytvoření adresáře
```
4 bytes: příkaz "mkdr"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
8 bytes: mtime_ns
4 bytes: mode
4 bytes: uid
4 bytes: gid
2 bytes: délka owner_name
N bytes: owner_name
2 bytes: délka group_name
N bytes: group_name
```

#### `syml` - vytvoření symbolického odkazu
```
4 bytes: příkaz "syml"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
2 bytes: délka cíle odkazu (UTF-8)
N bytes: cíl odkazu
8 bytes: mtime_ns
4 bytes: uid
4 bytes: gid
```

#### `dele` - smazání souboru/adresáře na cíli
```
4 bytes: příkaz "dele"
2 bytes: délka relativní cesty (UTF-8)
N bytes: relativní cesta
1 byte:  typ (0=soubor, 1=adresář, 2=symlink)
```

---

## Detailní popis průběhu synchronizace

### Krok 1: checksum strana pošle seznam souborů na cíli

```python
def do_checksum_recursive(dir_path, output_stream):
    # 1. Projít adresář rekurzivně
    entries = []
    for root, dirs, files in os.walk(dir_path):
        for d in dirs:
            entries.append(('dirs', relative_path, stat))
        for f in files:
            if is_symlink:
                entries.append(('link', relative_path, target, stat))
            else:
                entries.append(('file', relative_path, stat))

    # 2. Odeslat seznam
    send_command('list', len(entries))
    for entry in sorted(entries):
        send_entry(entry)
    send_command('ends')

    # 3. Čekat na požadavky na checksums
    while True:
        cmd = read_command()
        if cmd == 'getc':
            path = read_path()
            # Odeslat checksums pro tento soubor
            do_checksum_single_file(dir_path / path, output_stream)
        elif cmd == 'done':
            break
```

### Krok 2: retrieve strana porovná a odešle změny

```python
def do_retrieve_recursive(dir_path, input_stream, output_stream):
    # 1. Přijmout seznam souborů z cíle
    dst_entries = receive_file_list(input_stream)

    # 2. Projít zdrojový adresář
    src_entries = scan_directory(dir_path)

    # 3. Porovnat a rozhodnout o akcích
    actions = []

    # Soubory jen na zdroji - přidat
    for path in src_entries - dst_entries:
        actions.append(('add', path))

    # Soubory na obou stranách - porovnat
    for path in src_entries & dst_entries:
        if needs_update(src_entries[path], dst_entries[path]):
            actions.append(('update', path))

    # Soubory jen na cíli - smazat (pokud --delete)
    if delete_mode:
        for path in dst_entries - src_entries:
            actions.append(('delete', path))

    # 4. Provést akce
    for action, path in sorted(actions):
        if action == 'add':
            send_new_file(path)
        elif action == 'update':
            # Vyžádat checksums
            request_checksums(path)
            checksums = receive_checksums()
            send_diff(path, checksums)
        elif action == 'delete':
            send_delete(path)

    send_done()
```

### Krok 3: save strana aplikuje změny

```python
def do_save_recursive(dir_path, input_stream):
    while True:
        cmd = read_command()

        if cmd == 'file':
            path = read_path()
            file_path = dir_path / path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            receive_and_write_file(file_path)

        elif cmd == 'mkdr':
            path, meta = read_mkdir_data()
            (dir_path / path).mkdir(parents=True, exist_ok=True)
            apply_metadata(dir_path / path, meta)

        elif cmd == 'syml':
            path, target, meta = read_symlink_data()
            (dir_path / path).symlink_to(target)
            apply_metadata(dir_path / path, meta)

        elif cmd == 'dele':
            path, type = read_delete_data()
            if type == 'file':
                (dir_path / path).unlink()
            elif type == 'dir':
                shutil.rmtree(dir_path / path)

        elif cmd == 'done':
            break
```

---

## Nové CLI argumenty

### checksum
```
--recursive, -r     Rekurzivně procházet adresáře
```

### retrieve
```
--recursive, -r     Rekurzivně procházet adresáře
--delete            Smazat soubory na cíli, které nejsou na zdroji
--exclude PATTERN   Vyloučit soubory odpovídající vzoru (lze opakovat)
--include PATTERN   Zahrnout pouze soubory odpovídající vzoru
```

### save
```
--recursive, -r     Rekurzivně ukládat soubory
--dry-run           Pouze vypsat, co by se provedlo
```

---

## Rozhodnutí o rychlé cestě (fast path)

Pro optimalizaci lze použít rychlou cestu založenou na porovnání mtime a velikosti:

```python
def needs_update(src_stat, dst_stat):
    # Rychlá cesta: pokud se mtime a velikost shodují, soubor je pravděpodobně stejný
    if src_stat.st_size == dst_stat.st_size and src_stat.st_mtime_ns == dst_stat.st_mtime_ns:
        return False
    return True
```

Pro přísnější kontrolu lze přidat flag `--checksum`, který vždy porovnává hashe (jako rsync).

---

## Ošetření speciálních případů

### 1. Prázdné adresáře
- Prázdné adresáře jsou zahrnuty ve výpisu jako `dirs` příkazy
- Vytvoří se pomocí `mkdr` příkazu

### 2. Symbolické odkazy
- Přenáší se cíl odkazu, ne obsah
- Na cíli se vytvoří nový symlink

### 3. Hardlinky
- Hardlinky jsou přenášeny jako samostatné soubory (bez deduplikace)
- Budoucí rozšíření: detekce hardlinků a jejich zachování

### 4. Speciální soubory (devices, sockets, pipes)
- Ve výchozím nastavení se přeskakují s varováním
- Flag `--devices` by umožnil přenos device nodes

### 5. Sparse files
- Aktuálně se nepodporují speciálně
- Budoucí rozšíření: detekce a zachování sparse bloků

### 6. Soubory s mezerami/speciálními znaky v názvu
- Cesty jsou kódovány jako UTF-8 s explicitní délkou
- Žádné escape sekvence nejsou potřeba

### 7. Přístupová práva
- Při vytváření adresářů se dočasně nastaví 0o700, finální práva se nastaví až na konci
- Toto umožní zápis do adresáře i když cílová práva jsou pouze pro čtení

### 8. Maximální délka cesty
- Protokol podporuje cesty do 65535 bytů (2-byte délka)
- Prakticky omezeno OS limitem (typicky 4096 bytů)

---

## Návrh testů

### Jednotkové testy

#### test_recursive_list.py
```python
def test_list_empty_directory():
    """Test výpisu prázdného adresáře."""

def test_list_single_file():
    """Test výpisu adresáře s jedním souborem."""

def test_list_nested_directories():
    """Test výpisu vnořených adresářů."""

def test_list_with_symlinks():
    """Test výpisu adresáře obsahujícího symlinky."""

def test_list_special_characters_in_names():
    """Test výpisu souborů se speciálními znaky v názvech."""

def test_list_unicode_names():
    """Test výpisu souborů s Unicode znaky v názvech."""
```

#### test_recursive_protocol.py
```python
def test_file_command_encoding():
    """Test kódování file příkazu."""

def test_dirs_command_encoding():
    """Test kódování dirs příkazu."""

def test_link_command_encoding():
    """Test kódování link příkazu."""

def test_dele_command_encoding():
    """Test kódování dele příkazu."""

def test_mkdr_command_encoding():
    """Test kódování mkdr příkazu."""
```

### Integrační testy

#### test_recursive_copy.py
```python
def test_copy_empty_directory():
    """Kopírování prázdného adresáře."""

def test_copy_single_file_in_directory():
    """Kopírování adresáře s jedním souborem."""

def test_copy_nested_directories():
    """Kopírování vnořené adresářové struktury."""

def test_copy_with_symlinks():
    """Kopírování adresáře se symlinky."""

def test_copy_preserves_permissions():
    """Test zachování práv při kopírování."""

def test_copy_preserves_timestamps():
    """Test zachování časových značek."""

def test_copy_preserves_owner_group():
    """Test zachování vlastníka a skupiny."""

def test_incremental_copy():
    """Test inkrementálního kopírování - pouze změněné soubory."""

def test_copy_new_files_only():
    """Test přidání nových souborů bez změny existujících."""

def test_copy_modified_files():
    """Test aktualizace změněných souborů."""

def test_copy_identical_directories():
    """Test kopírování identických adresářů - žádný přenos dat."""

@mark.parametrize('use_lzma', [False, True])
def test_copy_large_files_recursive(use_lzma):
    """Test kopírování velkých souborů v rekurzivním režimu."""
```

#### test_recursive_delete.py
```python
def test_delete_extra_files():
    """Test mazání souborů na cíli, které nejsou na zdroji."""

def test_delete_extra_directories():
    """Test mazání adresářů na cíli, které nejsou na zdroji."""

def test_delete_nested_extra_content():
    """Test mazání vnořeného obsahu navíc."""

def test_no_delete_without_flag():
    """Test že bez --delete se soubory nemažou."""
```

#### test_recursive_edge_cases.py
```python
def test_file_becomes_directory():
    """Test kdy soubor na zdroji nahradí adresář na cíli."""

def test_directory_becomes_file():
    """Test kdy adresář na zdroji nahradí soubor na cíli."""

def test_symlink_becomes_file():
    """Test kdy symlink na zdroji nahradí soubor na cíli."""

def test_circular_symlinks():
    """Test ošetření cyklických symlinků."""

def test_broken_symlinks():
    """Test kopírování nefunkčních symlinků."""

def test_deep_directory_structure():
    """Test velmi hluboké adresářové struktury."""

def test_many_small_files():
    """Test adresáře s mnoha malými soubory."""

def test_interrupted_copy():
    """Test přerušeného a znovu spuštěného kopírování."""
```

#### test_recursive_filters.py
```python
def test_exclude_pattern():
    """Test vyloučení souborů podle vzoru."""

def test_exclude_directory():
    """Test vyloučení celého adresáře."""

def test_include_pattern():
    """Test zahrnutí pouze souborů odpovídajících vzoru."""

def test_multiple_exclude_patterns():
    """Test více exclude vzorů."""
```

---

## Postup implementace

### Etapa 1: Základní infrastruktura
1. Přidat `--recursive` flag do CLI parseru
2. Implementovat skenování adresáře
3. Implementovat nové protokolové příkazy pro seznam souborů

### Etapa 2: Jednostranný přenos (nové soubory)
1. Implementovat přenos nových souborů
2. Implementovat vytváření adresářů
3. Implementovat vytváření symlinků

### Etapa 3: Inkrementální přenos
1. Implementovat porovnání seznamů souborů
2. Implementovat vyžádání checksums pro konkrétní soubory
3. Implementovat přenos pouze změněných bloků

### Etapa 4: Mazání a metadata
1. Implementovat `--delete` flag
2. Implementovat mazání souborů a adresářů
3. Implementovat zachování všech metadat (práva, časy, vlastník)

### Etapa 5: Pokročilé funkce
1. Implementovat `--exclude` a `--include` filtry
2. Implementovat `--dry-run`
3. Optimalizovat pro velké množství souborů

---

## Kompatibilita

### Zpětná kompatibilita
Tento návrh **není zpětně kompatibilní** se stávající verzí. Při použití `--recursive` flagu bude vyžadována nová verze na obou stranách (checksum/retrieve/save).

Bez `--recursive` flagu zůstává chování identické se stávající verzí.

### Detekce verze
Pro robustnější detekci nekompatibility lze přidat handshake na začátek komunikace:

```
4 bytes: "BCPY"  (magic)
2 bytes: verze protokolu (major)
2 bytes: verze protokolu (minor)
4 bytes: flags (recursive=1, delete=2, ...)
```

---

## Rizika a omezení

1. **Paměťová náročnost**: Při velkém množství souborů může seznam zabrat značnou paměť
   - Řešení: Streamované zpracování, omezení hloubky

2. **Race conditions**: Soubory se mohou měnit během synchronizace
   - Řešení: Varování při detekci změny, možnost retry

3. **Oprávnění**: Vytváření souborů s omezenými právy může selhat
   - Řešení: Dočasná úprava práv, aplikace finálních práv na konci

4. **Atomicita**: Přerušená synchronizace může zanechat nekonzistentní stav
   - Řešení: `--partial` flag, dočasné soubory, finální přejmenování
