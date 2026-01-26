# Plán implementace rekurzivní synchronizace adresářů (--recursive)

## Přehled

Tento dokument popisuje návrh implementace volby `--recursive` pro nástroj blockcopy, která umožní synchronizaci celých adresářových stromů. Změny nejsou zpětně kompatibilní s verzí 0.0.3.

## Základní principy

1. **Jednosměrný tok dat** - zachován stávající model: checksum → retrieve → save
2. **Přenos pouze rozdílů** - přečtou se a zahashují všechny soubory na obou stranách, přenesou se jen změněné bloky
3. **Sekvenční zpracování souborů** - soubory se zpracovávají jeden po druhém v abecedním pořadí cest (zajišťuje deterministické chování)

## Návrh protokolu

### Společné konstanty

```python
# Typy souborových položek
FTYPE_FILE    = 0x01  # běžný soubor
FTYPE_DIR     = 0x02  # adresář
FTYPE_SYMLINK = 0x03  # symbolický odkaz
FTYPE_ABSENT  = 0x00  # neexistuje (pro smazání)

# Příznaky akcí
FLAG_UNCHANGED = 0x00  # beze změny
FLAG_NEW       = 0x01  # nový soubor/adresář
FLAG_MODIFIED  = 0x02  # změněný obsah
FLAG_DELETE    = 0x04  # smazat (existuje na dest, neexistuje na source)
FLAG_META_ONLY = 0x08  # změnit pouze metadata
```

### Protokol checksum → retrieve

Checksum projde cílový adresář a pro každou položku odešle informace:

#### Příkaz `entr` (entry) - začátek položky

```
entr           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
file_type      (1 byte)   - typ položky (FTYPE_*)
file_size      (8 bytes)  - velikost souboru (big-endian), 0 pro adresáře/symlinky
link_target_len (4 bytes) - délka cíle symlinku (0 pokud není symlink)
link_target    (N bytes)  - cíl symlinku (UTF-8), prázdné pokud není symlink
```

#### Příkaz `Hash` (beze změny) - hash bloku

Pouze pro běžné soubory s `file_size > 0`:

```
Hash           (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru (big-endian)
block_size     (4 bytes)  - velikost bloku (big-endian)
block_hash     (64 bytes) - SHA3-512 digest
```

#### Příkaz `eent` (end entry) - konec položky

```
eent           (4 bytes)  - command identifier
```

#### Příkaz `done` (beze změny) - konec celého přenosu

```
done           (4 bytes)  - command identifier
```

#### Příklad toku dat (checksum)

```
entr + "src/main.py" + FTYPE_FILE + 1024 + 0 + ""
  Hash + 0 + 131072 + <hash1>
  ... (další hashe pokud je soubor větší)
eent
entr + "src/utils/" + FTYPE_DIR + 0 + 0 + ""
eent
entr + "src/utils/helper.py" + FTYPE_FILE + 512 + 0 + ""
  Hash + 0 + 512 + <hash2>
eent
entr + "link_to_readme" + FTYPE_SYMLINK + 0 + 9 + "README.md"
eent
done
```

### Protokol retrieve → save

Retrieve čte hashe ze stdin, porovnává se zdrojovými soubory a odesílá změny:

#### Příkaz `item` - začátek položky

```
item           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech
path           (N bytes)  - relativní cesta (UTF-8)
file_type      (1 byte)   - typ položky (FTYPE_*)
flags          (1 byte)   - příznaky akce (FLAG_*)
```

#### Příkaz `data` (beze změny) - datový blok

Pouze pro soubory s `FLAG_NEW` nebo `FLAG_MODIFIED`:

```
data           (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru
block_size     (4 bytes)  - velikost dat
block_data     (N bytes)  - raw data
```

#### Příkaz `dlzm` (beze změny) - komprimovaný datový blok

```
dlzm           (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru
block_size     (4 bytes)  - velikost komprimovaných dat
block_data     (N bytes)  - LZMA komprimovaná data
```

#### Příkaz `meta` - metadata položky

Rozšířeno o podporu různých typů:

```
meta           (4 bytes)  - command identifier
atime_ns       (8 bytes)  - access time (nanoseconds, signed)
mtime_ns       (8 bytes)  - modification time (nanoseconds, signed)
mode           (4 bytes)  - file mode/permissions
uid            (4 bytes)  - user id
gid            (4 bytes)  - group id
owner_name_len (2 bytes)  - délka jména vlastníka
owner_name     (N bytes)  - jméno vlastníka (UTF-8)
group_name_len (2 bytes)  - délka jména skupiny
group_name     (N bytes)  - jméno skupiny (UTF-8)
total_size     (8 bytes)  - velikost souboru
link_target_len (4 bytes) - délka cíle symlinku (0 pokud není symlink)
link_target    (N bytes)  - cíl symlinku
end            (3 bytes)  - literal "end" marker
```

#### Příkaz `eoit` (end of item) - konec položky

```
eoit           (4 bytes)  - command identifier
```

#### Příkaz `done` (beze změny) - konec přenosu

```
done           (4 bytes)  - command identifier
```

#### Příklad toku dat (retrieve)

```
# Nový soubor
item + "src/new_file.py" + FTYPE_FILE + FLAG_NEW
  data + 0 + 1024 + <data>
  meta + <metadata>
eoit

# Změněný soubor (jen některé bloky)
item + "src/main.py" + FTYPE_FILE + FLAG_MODIFIED
  data + 131072 + 512 + <changed_block>
  meta + <metadata>
eoit

# Beze změny (jen metadata se mohou aktualizovat)
item + "src/utils/helper.py" + FTYPE_FILE + FLAG_UNCHANGED
  meta + <metadata>
eoit

# Nový adresář
item + "src/new_dir/" + FTYPE_DIR + FLAG_NEW
  meta + <metadata>
eoit

# Smazat soubor (existuje na dest, ne na source)
item + "src/old_file.py" + FTYPE_ABSENT + FLAG_DELETE
eoit

done
```

## Algoritmus zpracování

### Fáze checksum

```
1. Pokud --recursive:
   a. Rekurzivně projít cílový adresář
   b. Seřadit položky podle cesty (abecedně)
   c. Pro každou položku:
      - Odeslat příkaz 'entr' s informacemi o položce
      - Pokud je to soubor, odeslat hashe všech bloků
      - Odeslat příkaz 'eent'
   d. Odeslat příkaz 'done'
2. Jinak (původní chování):
   - Zpracovat jako dosud (jeden soubor)
```

### Fáze retrieve

```
1. Pokud --recursive:
   a. Načíst manifest položek ze source adresáře
   b. Číst příkazy ze stdin (checksum výstup)
   c. Pro každý 'entr':
      - Najít odpovídající source položku
      - Porovnat typ a obsah
      - Určit akci (FLAG_*)
   d. Po 'done' ze stdin:
      - Identifikovat source položky bez dest protějšku (nové)
   e. Odeslat všechny položky s příslušnými akcemi
   f. Odeslat 'done'
2. Jinak (původní chování):
   - Zpracovat jako dosud
```

### Fáze save

```
1. Pokud --recursive:
   a. Zpracovat příkazy ze stdin
   b. Pro každý 'item':
      - FLAG_NEW + FTYPE_DIR: vytvořit adresář
      - FLAG_NEW + FTYPE_FILE: vytvořit soubor, zapsat data
      - FLAG_NEW + FTYPE_SYMLINK: vytvořit symlink
      - FLAG_MODIFIED: zapsat změněné bloky
      - FLAG_DELETE: smazat položku
      - FLAG_META_ONLY: aktualizovat pouze metadata
   c. Aplikovat metadata podle --times, --perms, --owner, --group
   d. Po 'done' skončit
2. Jinak (původní chování):
   - Zpracovat jako dosud
```

## Nové CLI volby

### Volba `--recursive` / `-r`

```
blockcopy checksum --recursive /path/to/dest_dir | \
  ssh srchost blockcopy retrieve --recursive /path/to/source_dir | \
  blockcopy save --recursive /path/to/dest_dir
```

### Volba `--delete`

Smazat soubory na dest, které neexistují na source:

```
blockcopy save --recursive --delete /path/to/dest_dir
```

**Výchozí chování**: bez `--delete` se soubory na dest zachovávají.

### Volba `--exclude` (budoucí rozšíření)

Vyloučit soubory podle vzoru:

```
blockcopy checksum --recursive --exclude "*.pyc" --exclude "__pycache__" /path/to/dest
```

Poznámka: Implementace `--exclude` může být přidána v budoucí verzi.

## Řešení okrajových případů

### Prázdné adresáře

- Přenášet i prázdné adresáře jako `FTYPE_DIR` položky
- Save vytvoří adresář pomocí `os.makedirs(exist_ok=True)`

### Symlinky

- Přenášet jako `FTYPE_SYMLINK` s cílem odkazu
- Na dest se vytvoří pomocí `os.symlink()`
- Nesledovat symlinky při procházení (použít `os.walk(followlinks=False)`)

### Změna typu položky

Pokud se typ položky změní (soubor → adresář, symlink → soubor, atd.):

1. Smazat původní položku na dest
2. Vytvořit novou položku správného typu

V protokolu: poslat `FLAG_DELETE` pro starou položku, pak `FLAG_NEW` pro novou.

### Soubory s nulovou velikostí

- Přenášet jako běžné soubory bez hashů
- `entr` bude mít `file_size = 0`
- Nepošlou se žádné `Hash` příkazy
- V retrieve se pošle `FLAG_NEW` nebo `FLAG_UNCHANGED` podle existence

### Oprávnění a přístup

- Pokud nelze přečíst soubor (PermissionError), vypsat varování na stderr a přeskočit
- Volba `--ignore-errors` pro pokračování i při chybách (budoucí rozšíření)

### Pořadí vytváření

Adresáře se musí vytvářet před soubory, které obsahují:

- Položky seřadit tak, aby rodiče předcházeli potomkům
- Prakticky: seřadit podle cesty zajistí správné pořadí (`a/` před `a/b.txt`)

### Pořadí mazání (--delete)

Soubory a adresáře mazat v opačném pořadí (potomci před rodiči):

- Příkazy `FLAG_DELETE` seřadit sestupně podle hloubky cesty
- Nebo mazat rekurzivně pomocí `shutil.rmtree()` pro adresáře

## Paralelizace

### Fáze checksum

Stávající model ThreadPoolExecutor zachovat:

- 1 vlákno pro procházení adresáře a čtení souborů
- N vláken pro výpočet hashů
- 1 vlákno pro odesílání výstupu

### Fáze retrieve

Podobný model:

- 1 vlákno pro čtení source souborů
- N vláken pro porovnávání hashů a přípravu dat
- 1 vlákno pro odesílání výstupu

### Fáze save

Jednověláknové zpracování (jako dosud) - zápis musí být sekvenční.

## Změny ve struktuře kódu

### Nové funkce

```python
def walk_directory(path: Path) -> Iterator[Tuple[Path, int, int, str]]:
    """
    Rekurzivně projde adresář a vrátí informace o položkách.
    Yields: (relativní_cesta, typ, velikost, link_target)
    """

def read_directory_manifest(path: Path) -> Dict[str, DirEntry]:
    """
    Načte manifest zdrojového adresáře do paměti.
    """

def send_entry_header(out: IO, path: str, file_type: int, size: int, link_target: str):
    """
    Odešle příkaz 'entr'.
    """

def send_end_entry(out: IO):
    """
    Odešle příkaz 'eent'.
    """

def send_item_header(out: IO, path: str, file_type: int, flags: int):
    """
    Odešle příkaz 'item'.
    """

def send_end_item(out: IO):
    """
    Odešle příkaz 'eoit'.
    """
```

### Úpravy stávajících funkcí

- `main_checksum()` - přidat větev pro `--recursive`
- `main_retrieve()` - přidat větev pro `--recursive`
- `main_save()` - přidat větev pro `--recursive`
- `get_argument_parser()` - přidat nové argumenty

### Nové třídy

```python
@dataclass
class DirEntry:
    """Reprezentuje položku v adresáři."""
    path: str           # relativní cesta
    file_type: int      # FTYPE_*
    size: int           # velikost (0 pro adresáře/symlinky)
    link_target: str    # cíl symlinku (prázdný pro ostatní)
    mode: int
    uid: int
    gid: int
    atime_ns: int
    mtime_ns: int
```

## Návrh testů

### Jednotkové testy

#### test_recursive_protocol.py

```python
def test_entry_command_encoding():
    """Test správného kódování příkazu 'entr'."""

def test_entry_command_decoding():
    """Test správného dekódování příkazu 'entr'."""

def test_item_command_encoding():
    """Test správného kódování příkazu 'item'."""

def test_item_command_decoding():
    """Test správného dekódování příkazu 'item'."""

def test_extended_meta_encoding():
    """Test kódování rozšířeného příkazu 'meta' se symlink target."""

def test_walk_directory_order():
    """Test že walk_directory vrací položky v abecedním pořadí."""

def test_walk_directory_types():
    """Test správné detekce typů (soubor, adresář, symlink)."""
```

### Integrační testy

#### test_recursive_copy.py

```python
def test_recursive_copy_basic():
    """
    Test základní rekurzivní kopie.
    - Vytvoř source adresář s několika soubory a podadresáři
    - Spusť pipeline s --recursive
    - Ověř že dest odpovídá source
    """

def test_recursive_copy_incremental():
    """
    Test inkrementální kopie.
    - Vytvoř identický source a dest
    - Změň jeden soubor na source
    - Spusť pipeline
    - Ověř že se přenesl jen změněný blok
    """

def test_recursive_copy_new_files():
    """
    Test přidání nových souborů.
    - Vytvoř source s více soubory než dest
    - Spusť pipeline
    - Ověř že nové soubory existují na dest
    """

def test_recursive_copy_delete():
    """
    Test mazání souborů s --delete.
    - Vytvoř dest s více soubory než source
    - Spusť pipeline s --delete
    - Ověř že soubory neexistující na source byly smazány
    """

def test_recursive_copy_no_delete():
    """
    Test zachování souborů bez --delete.
    - Vytvoř dest s více soubory než source
    - Spusť pipeline BEZ --delete
    - Ověř že extra soubory na dest zůstaly
    """

def test_recursive_copy_empty_directory():
    """
    Test kopie prázdných adresářů.
    """

def test_recursive_copy_symlinks():
    """
    Test kopie symbolických odkazů.
    - Absolutní symlink
    - Relativní symlink
    - Broken symlink
    """

def test_recursive_copy_type_change():
    """
    Test změny typu položky.
    - Soubor → adresář
    - Adresář → soubor
    - Soubor → symlink
    """

def test_recursive_copy_permissions():
    """
    Test zachování oprávnění.
    - Různá oprávnění souborů
    - Oprávnění adresářů
    """

def test_recursive_copy_timestamps():
    """
    Test zachování časových razítek.
    - mtime
    - atime (pokud --times)
    """

def test_recursive_copy_large_directory():
    """
    Test s velkým počtem souborů.
    - 1000+ souborů
    - Hluboká hierarchie (10+ úrovní)
    """

def test_recursive_copy_special_names():
    """
    Test souborů se speciálními názvy.
    - Mezery v názvech
    - Unicode znaky
    - Velmi dlouhé názvy
    """

def test_recursive_copy_zero_size_files():
    """
    Test prázdných souborů.
    """

def test_recursive_copy_with_lzma():
    """
    Test rekurzivní kopie s LZMA kompresí.
    """

def test_recursive_copy_progress():
    """
    Test progress výstupu při rekurzivní kopii.
    - Zobrazení aktuálního souboru
    - Zobrazení celkového postupu
    """
```

### Testy kompatibility

#### test_recursive_rsync_compat.py

```python
def test_recursive_rsync_identical():
    """
    Po blockcopy --recursive by rsync neměl najít žádné rozdíly.
    """

def test_recursive_rsync_permissions():
    """
    Ověř že oprávnění odpovídají rsync -a.
    """

def test_recursive_rsync_timestamps():
    """
    Ověř že časová razítka odpovídají rsync -a.
    """
```

### Testy chybových stavů

#### test_recursive_errors.py

```python
def test_recursive_permission_denied():
    """
    Test chování při chybě přístupu k souboru.
    """

def test_recursive_broken_symlink():
    """
    Test kopie nefunkčního symlinku.
    """

def test_recursive_interrupted():
    """
    Test přerušení uprostřed kopie.
    """

def test_recursive_disk_full():
    """
    Test chování při plném disku na dest.
    """
```

## Plán implementace

### Fáze 1: Protokol a základní struktury

1. Definovat nové příkazy protokolu (konstanty)
2. Implementovat `DirEntry` dataclass
3. Implementovat funkce pro kódování/dekódování nových příkazů
4. Napsat jednotkové testy protokolu

### Fáze 2: Checksum s --recursive

1. Implementovat `walk_directory()`
2. Rozšířit `main_checksum()` o větev pro `--recursive`
3. Zachovat existující paralelizaci pro hashování
4. Napsat testy pro checksum fázi

### Fáze 3: Retrieve s --recursive

1. Implementovat `read_directory_manifest()`
2. Rozšířit `main_retrieve()` o větev pro `--recursive`
3. Implementovat porovnávání a určení akcí
4. Napsat testy pro retrieve fázi

### Fáze 4: Save s --recursive

1. Rozšířit `main_save()` o větev pro `--recursive`
2. Implementovat vytváření adresářů a symlinků
3. Implementovat volbu `--delete`
4. Napsat testy pro save fázi

### Fáze 5: Integrační testy a ladění

1. Napsat kompletní integrační testy
2. Provést testy s reálnými daty
3. Optimalizovat výkon
4. Dokumentace

## Poznámky k výkonu

### Paměťové nároky

- Manifest source adresáře se načte celý do paměti
- Pro velmi velké adresáře (miliony souborů) zvážit streaming přístup
- Odhad: 1M souborů ≈ 200-500 MB RAM pro manifest

### I/O optimalizace

- Soubory číst sekvenčně (lepší pro HDD)
- Zachovat stávající buffer velikosti (128 KB bloky)
- Pro SSD/NVMe by šlo zvýšit paralelizaci čtení souborů

### Síťová optimalizace

- LZMA komprese výrazně sníží přenos dat
- Zvážit volbu `--compress-level` pro LZMA

## Budoucí rozšíření (mimo scope tohoto plánu)

1. `--exclude` / `--include` vzory
2. `--dry-run` pro simulaci
3. `--checksum` pro vynucení porovnání hashů i při shodné velikosti/mtime
4. `--ignore-existing` pro přeskočení existujících souborů
5. Podpora rozšířených atributů (xattr)
6. Podpora ACL
7. Podpora sparse souborů
8. Obousměrná synchronizace (mimo scope - komplikuje jednosměrný tok)
