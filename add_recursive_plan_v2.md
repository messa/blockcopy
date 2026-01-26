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
# Příznaky akcí (používané v protokolu retrieve → save)
FLAG_UNCHANGED = 0x00  # beze změny
FLAG_NEW       = 0x01  # nový soubor/adresář
FLAG_MODIFIED  = 0x02  # změněný obsah
FLAG_DELETE    = 0x04  # smazat (existuje na dest, neexistuje na source)
FLAG_META_ONLY = 0x08  # změnit pouze metadata
```

**Poznámka k symlinkům:** Symlinky se následují (followlinks=True). Přenáší se obsah cílového souboru/adresáře, nikoliv samotný symlink. Tím se zjednodušuje protokol a chování je konzistentní.

### Protokol checksum → retrieve

Checksum projde cílový adresář a pro každou položku odešle informace. Používají se dva příkazy podle typu položky:

#### Příkaz `file` - soubor

```
file           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
file_size      (8 bytes)  - velikost souboru (big-endian)
```

Po příkazu `file` následují hashe všech bloků (pokud `file_size > 0`):

#### Příkaz `Hash` (beze změny) - hash bloku

```
Hash           (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru (big-endian)
block_size     (4 bytes)  - velikost bloku (big-endian)
block_hash     (64 bytes) - SHA3-512 digest
```

Po posledním hashi následuje konec souboru:

#### Příkaz `endf` (end file) - konec souboru

```
endf           (4 bytes)  - command identifier
```

#### Příkaz `dire` - adresář

```
dire           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
```

Adresáře nemají žádná následující data ani ukončovací příkaz.

#### Příkaz `done` (beze změny) - konec celého přenosu

```
done           (4 bytes)  - command identifier
```

#### Příklad toku dat (checksum)

```
file + "src/main.py" + 1024
  Hash + 0 + 131072 + <hash1>
  ... (další hashe pokud je soubor větší)
endf
dire + "src/utils/"
file + "src/utils/helper.py" + 512
  Hash + 0 + 512 + <hash2>
endf
done
```

### Protokol retrieve → save

Retrieve čte hashe ze stdin, porovnává se zdrojovými soubory a odesílá změny. Podobně jako checksum používá oddělené příkazy pro soubory a adresáře:

#### Příkaz `file` - soubor

```
file           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech
path           (N bytes)  - relativní cesta (UTF-8)
flags          (1 byte)   - příznaky akce (FLAG_*)
```

Po příkazu `file` následují datové bloky (pokud `FLAG_NEW` nebo `FLAG_MODIFIED`):

#### Příkaz `data` (beze změny) - datový blok

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

#### Příkaz `meta` - metadata souboru

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
end            (3 bytes)  - literal "end" marker
```

#### Příkaz `endf` (end file) - konec souboru

```
endf           (4 bytes)  - command identifier
```

#### Příkaz `dire` - adresář

```
dire           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech
path           (N bytes)  - relativní cesta (UTF-8)
flags          (1 byte)   - příznaky akce (FLAG_*)
```

Pro adresáře následuje pouze metadata (pokud `FLAG_NEW` nebo `FLAG_META_ONLY`):

#### Příkaz `dmet` - metadata adresáře

```
dmet           (4 bytes)  - command identifier
atime_ns       (8 bytes)  - access time (nanoseconds, signed)
mtime_ns       (8 bytes)  - modification time (nanoseconds, signed)
mode           (4 bytes)  - directory mode/permissions
uid            (4 bytes)  - user id
gid            (4 bytes)  - group id
owner_name_len (2 bytes)  - délka jména vlastníka
owner_name     (N bytes)  - jméno vlastníka (UTF-8)
group_name_len (2 bytes)  - délka jména skupiny
group_name     (N bytes)  - jméno skupiny (UTF-8)
end            (3 bytes)  - literal "end" marker
```

Adresáře nemají ukončovací příkaz (metadata jsou volitelná a příkaz `dmet` je ukončuje).

#### Příkaz `remv` - smazání položky

Pro mazání souborů i adresářů (s `--delete`):

```
remv           (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech
path           (N bytes)  - relativní cesta (UTF-8)
```

#### Příkaz `done` (beze změny) - konec přenosu

```
done           (4 bytes)  - command identifier
```

#### Příklad toku dat (retrieve)

```
# Nový soubor
file + "src/new_file.py" + FLAG_NEW
  data + 0 + 1024 + <data>
  meta + <metadata>
endf

# Změněný soubor (jen některé bloky)
file + "src/main.py" + FLAG_MODIFIED
  data + 131072 + 512 + <changed_block>
  meta + <metadata>
endf

# Beze změny (přeskočí se nebo jen meta update)
file + "src/utils/helper.py" + FLAG_UNCHANGED
  meta + <metadata>
endf

# Nový adresář
dire + "src/new_dir/" + FLAG_NEW
  dmet + <metadata>

# Existující adresář beze změny (metadata update)
dire + "src/utils/" + FLAG_META_ONLY
  dmet + <metadata>

# Smazat soubor (existuje na dest, ne na source)
remv + "src/old_file.py"

# Smazat adresář
remv + "src/old_dir/"

done
```

## Algoritmus zpracování

### Fáze checksum

```
1. Pokud --recursive:
   a. Rekurzivně projít cílový adresář (followlinks=True)
   b. Seřadit položky podle cesty (abecedně)
   c. Pro každou položku:
      - Adresář: odeslat příkaz 'dire'
      - Soubor: odeslat příkaz 'file', pak hashe všech bloků, pak 'endf'
   d. Odeslat příkaz 'done'
2. Jinak (původní chování):
   - Zpracovat jako dosud (jeden soubor)
```

### Fáze retrieve

```
1. Pokud --recursive:
   a. Načíst manifest položek ze source adresáře (followlinks=True)
   b. Číst příkazy ze stdin (checksum výstup)
   c. Pro každý 'file':
      - Najít odpovídající source soubor
      - Porovnat hashe bloků
      - Určit akci (FLAG_NEW, FLAG_MODIFIED, FLAG_UNCHANGED)
   d. Pro každý 'dire':
      - Najít odpovídající source adresář
      - Určit akci
   e. Po 'done' ze stdin:
      - Identifikovat source položky bez dest protějšku (nové)
      - Identifikovat dest položky bez source protějšku (k smazání)
   f. Odeslat všechny položky s příslušnými akcemi
   g. Odeslat příkazy 'remv' pro položky k smazání
   h. Odeslat 'done'
2. Jinak (původní chování):
   - Zpracovat jako dosud
```

### Fáze save

```
1. Pokud --recursive:
   a. Zpracovat příkazy ze stdin
   b. Pro každý 'dire':
      - FLAG_NEW: vytvořit adresář
      - Aplikovat metadata z 'dmet' (pokud následuje)
   c. Pro každý 'file':
      - FLAG_NEW: vytvořit soubor, zapsat data
      - FLAG_MODIFIED: zapsat změněné bloky
      - Aplikovat metadata z 'meta'
   d. Pro každý 'remv':
      - Smazat soubor nebo adresář (rekurzivně)
   e. Aplikovat metadata podle --times, --perms, --owner, --group
   f. Po 'done' skončit
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

- Přenášet i prázdné adresáře pomocí příkazu `dire`
- Save vytvoří adresář pomocí `os.makedirs(exist_ok=True)`

### Symlinky

Symlinky se následují (followlinks=True při os.walk). Toto znamená:

- Symlink na soubor: přenese se obsah cílového souboru pod cestou symlinku
- Symlink na adresář: rekurzivně se projde cílový adresář
- Broken symlink: přeskočí se s varováním na stderr (cíl neexistuje)

Výhody tohoto přístupu:
- Jednodušší protokol (není potřeba FTYPE_SYMLINK)
- Destination dostane skutečná data, ne závislost na lokálních cestách
- Konzistentní chování nezávislé na umístění source

### Změna typu položky

Pokud se typ položky změní (soubor → adresář nebo adresář → soubor):

1. Poslat příkaz `remv` pro starou položku
2. Poslat příkaz `file` nebo `dire` s `FLAG_NEW` pro novou položku

### Soubory s nulovou velikostí

- Přenášet jako běžné soubory bez hashů
- `file` bude mít `file_size = 0`
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
def walk_directory(path: Path, followlinks: bool = True) -> Iterator[Tuple[Path, bool, int]]:
    """
    Rekurzivně projde adresář a vrátí informace o položkách.
    Yields: (relativní_cesta, is_dir, velikost)

    Položky jsou seřazené abecedně podle cesty.
    Symlinky se následují (followlinks=True).
    """

def read_directory_manifest(path: Path) -> Dict[str, DirEntry]:
    """
    Načte manifest zdrojového adresáře do paměti.
    Klíč je relativní cesta, hodnota je DirEntry.
    """

def send_file_header(out: IO, path: str, size: int):
    """
    Odešle příkaz 'file' (checksum → retrieve).
    """

def send_file_header_with_flags(out: IO, path: str, flags: int):
    """
    Odešle příkaz 'file' (retrieve → save).
    """

def send_end_file(out: IO):
    """
    Odešle příkaz 'endf'.
    """

def send_dire_header(out: IO, path: str):
    """
    Odešle příkaz 'dire' (checksum → retrieve).
    """

def send_dire_header_with_flags(out: IO, path: str, flags: int):
    """
    Odešle příkaz 'dire' (retrieve → save).
    """

def send_dire_meta(out: IO, ...):
    """
    Odešle příkaz 'dmet' s metadaty adresáře.
    """

def send_remove(out: IO, path: str):
    """
    Odešle příkaz 'remv'.
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
    is_dir: bool        # True pro adresář, False pro soubor
    size: int           # velikost (0 pro adresáře)
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
def test_file_command_encoding():
    """Test správného kódování příkazu 'file' (checksum)."""

def test_file_command_decoding():
    """Test správného dekódování příkazu 'file'."""

def test_dire_command_encoding():
    """Test správného kódování příkazu 'dire'."""

def test_dire_command_decoding():
    """Test správného dekódování příkazu 'dire'."""

def test_file_with_flags_encoding():
    """Test kódování příkazu 'file' s příznaky (retrieve)."""

def test_remv_command_encoding():
    """Test kódování příkazu 'remv'."""

def test_meta_encoding():
    """Test kódování příkazu 'meta'."""

def test_dmet_encoding():
    """Test kódování příkazu 'dmet'."""

def test_walk_directory_order():
    """Test že walk_directory vrací položky v abecedním pořadí."""

def test_walk_directory_types():
    """Test správné detekce typů (soubor, adresář)."""

def test_walk_directory_follows_symlinks():
    """Test že walk_directory následuje symlinky."""
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

def test_recursive_copy_symlinks_followed():
    """
    Test že symlinky jsou následovány.
    - Symlink na soubor: přenese se obsah
    - Symlink na adresář: rekurzivně se projde obsah
    - Ověření že dest obsahuje skutečná data, ne symlinky
    """

def test_recursive_copy_broken_symlink_skipped():
    """
    Test že broken symlinky jsou přeskočeny s varováním.
    """

def test_recursive_copy_type_change():
    """
    Test změny typu položky.
    - Soubor → adresář
    - Adresář → soubor
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

def test_recursive_broken_symlink_warning():
    """
    Test že nefunkční symlink vypíše varování a je přeskočen.
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

1. Definovat nové příkazy protokolu (`file`, `dire`, `endf`, `dmet`, `remv`)
2. Implementovat `DirEntry` dataclass
3. Implementovat funkce pro kódování/dekódování nových příkazů
4. Napsat jednotkové testy protokolu

### Fáze 2: Checksum s --recursive

1. Implementovat `walk_directory()` s followlinks=True
2. Rozšířit `main_checksum()` o větev pro `--recursive`
3. Zachovat existující paralelizaci pro hashování
4. Napsat testy pro checksum fázi

### Fáze 3: Retrieve s --recursive

1. Implementovat `read_directory_manifest()`
2. Rozšířit `main_retrieve()` o větev pro `--recursive`
3. Implementovat porovnávání a určení akcí (FLAG_*)
4. Implementovat generování příkazů `remv` pro smazání
5. Napsat testy pro retrieve fázi

### Fáze 4: Save s --recursive

1. Rozšířit `main_save()` o větev pro `--recursive`
2. Implementovat vytváření adresářů
3. Implementovat zpracování příkazu `remv`
4. Implementovat volbu `--delete`
5. Napsat testy pro save fázi

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
5. `--no-dereference` pro kopírování symlinků jako symlinků (místo následování)
6. Podpora rozšířených atributů (xattr)
7. Podpora ACL
8. Podpora sparse souborů
9. Obousměrná synchronizace (mimo scope - komplikuje jednosměrný tok)
