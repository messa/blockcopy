# Plán implementace rekurzivní synchronizace adresářů (--recursive)

## Přehled

Tento dokument popisuje návrh implementace volby `--recursive` pro nástroj blockcopy, která umožní synchronizaci celých adresářových stromů. Změny nejsou zpětně kompatibilní s verzí 0.0.3.

## Základní principy

1. **Jednosměrný tok dat** - zachován stávající model: checksum → retrieve → save
2. **Přenos pouze rozdílů** - přečtou se a zahashují všechny soubory na obou stranách, přenesou se jen změněné bloky
3. **Deterministické pořadí** - položky se zpracovávají v abecedním pořadí relativních cest (lexikografické řazení UTF-8 bytů), což zajišťuje reprodukovatelné chování

## Návrh protokolu

**Formát čísel:** Všechna vícebytová čísla v protokolu jsou kódována jako big-endian (network byte order), stejně jako ve stávající verzi blockcopy.

**Poznámka k symlinkům:** Symlinky se následují (followlinks=True). Přenáší se obsah cílového souboru/adresáře, nikoliv samotný symlink. Tím se zjednodušuje protokol a chování je konzistentní.

### Protokol checksum → retrieve

Checksum projde cílový adresář a pro každou položku odešle informace. Používají se dva příkazy podle typu položky:

#### Příkaz `file` - soubor

```
"file"         (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
file_size      (8 bytes)  - velikost souboru (big-endian)
```

Po příkazu `file` následují hashe všech bloků (pokud `file_size > 0`):

#### Příkaz `Hash` (beze změny) - hash bloku

```
"Hash"         (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru (big-endian)
block_size     (4 bytes)  - velikost bloku (big-endian)
block_hash     (64 bytes) - SHA3-512 digest
```

Po posledním hashi následuje konec souboru:

#### Příkaz `endf` (end file) - konec souboru

```
"endf"         (4 bytes)  - command identifier
```

#### Příkaz `dire` - adresář

```
"dire"         (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8, bez trailing slash)
```

Adresáře nemají žádná následující data ani ukončovací příkaz.

#### Příkaz `done` (beze změny) - konec celého přenosu

```
"done"         (4 bytes)  - command identifier
```

#### Příklad toku dat (checksum)

```
dire + "" (path_len=0, root adresář)
file + "src/main.py" + 1024
  Hash + 0 + 131072 + <hash1>
  ... (další hashe pokud je soubor větší)
endf
dire + "src/utils"
file + "src/utils/helper.py" + 512
  Hash + 0 + 512 + <hash2>
endf
done
```

### Protokol retrieve → save

Retrieve čte hashe ze stdin, porovnává se zdrojovými soubory a odesílá změny. Podobně jako checksum používá oddělené příkazy pro soubory a adresáře:

#### Příkaz `file` - soubor

```
"file"         (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
```

Po příkazu `file` mohou následovat datové bloky (pro změněné/nové soubory) a vždy metadata:

#### Příkaz `data` (beze změny) - datový blok

```
"data"         (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru (big-endian)
block_size     (4 bytes)  - velikost dat (big-endian)
block_data     (N bytes)  - raw data
```

#### Příkaz `dlzm` (beze změny) - komprimovaný datový blok

```
"dlzm"         (4 bytes)  - command identifier
block_pos      (8 bytes)  - pozice v souboru (big-endian)
block_size     (4 bytes)  - velikost komprimovaných dat (big-endian)
block_data     (N bytes)  - LZMA komprimovaná data
```

#### Příkaz `meta` - metadata souboru

```
"meta"         (4 bytes)  - command identifier
atime_ns       (8 bytes)  - access time in nanoseconds (big-endian, signed)
mtime_ns       (8 bytes)  - modification time in nanoseconds (big-endian, signed)
mode           (4 bytes)  - file mode/permissions (big-endian)
uid            (4 bytes)  - user id (big-endian)
gid            (4 bytes)  - group id (big-endian)
owner_name_len (2 bytes)  - délka jména vlastníka (big-endian)
owner_name     (N bytes)  - jméno vlastníka (UTF-8)
group_name_len (2 bytes)  - délka jména skupiny (big-endian)
group_name     (N bytes)  - jméno skupiny (UTF-8)
total_size     (8 bytes)  - velikost souboru (big-endian)
"end"          (3 bytes)  - literal end marker
```

#### Příkaz `endf` (end file) - konec souboru

```
"endf"         (4 bytes)  - command identifier
```

#### Příkaz `dire` - adresář

```
"dire"         (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8, bez trailing slash)
```

Po příkazu `dire` následují metadata:

#### Příkaz `dmet` - metadata adresáře

```
"dmet"         (4 bytes)  - command identifier
atime_ns       (8 bytes)  - access time in nanoseconds (big-endian, signed)
mtime_ns       (8 bytes)  - modification time in nanoseconds (big-endian, signed)
mode           (4 bytes)  - directory mode/permissions (big-endian)
uid            (4 bytes)  - user id (big-endian)
gid            (4 bytes)  - group id (big-endian)
owner_name_len (2 bytes)  - délka jména vlastníka (big-endian)
owner_name     (N bytes)  - jméno vlastníka (UTF-8)
group_name_len (2 bytes)  - délka jména skupiny (big-endian)
group_name     (N bytes)  - jméno skupiny (UTF-8)
"end"          (3 bytes)  - literal end marker
```

Adresáře nemají ukončovací příkaz (příkaz `dmet` ukončuje sekvenci pro daný adresář).

#### Příkaz `remv` - smazání položky

Pro mazání souborů i adresářů (s `--delete`):

```
"remv"         (4 bytes)  - command identifier
path_len       (4 bytes)  - délka cesty v bajtech (big-endian)
path           (N bytes)  - relativní cesta (UTF-8)
```

#### Příkaz `done` (beze změny) - konec přenosu

```
"done"         (4 bytes)  - command identifier
```

#### Příklad toku dat (retrieve)

```
# Nový soubor (všechna data)
file + "src/new_file.py"
  data + 0 + 1024 + <data>
  meta + <metadata s total_size=1024>
endf

# Změněný soubor (jen některé bloky)
file + "src/main.py"
  data + 131072 + 512 + <changed_block>
  meta + <metadata s total_size>
endf

# Beze změny (jen metadata, žádná data)
file + "src/utils/helper.py"
  meta + <metadata>
endf

# Adresář
dire + "src/new_dir"
  dmet + <metadata>

dire + "src/utils"
  dmet + <metadata>

# Smazat soubor (existuje na dest, ne na source)
remv + "src/old_file.py"

# Smazat adresář
remv + "src/old_dir"

done
```

**Poznámka:** Save odvozuje akci z přijatých dat:
- Přišla `data` → zapsat bloky na dané pozice
- `meta.total_size` → truncate soubor na správnou velikost
- `dmet` → vytvořit adresář (pokud neexistuje) a nastavit metadata
- `remv` → smazat položku

## Algoritmus zpracování

### Fáze checksum

1. Pokud `--recursive`:
   a. Odeslat `'dire'` s prázdnou cestou (`path_len=0`) - root adresář
   b. Rekurzivně projít cílový adresář (`followlinks=True`)
   c. Seřadit položky podle cesty (abecedně)
   d. Pro každou položku:
      - Adresář: odeslat příkaz `'dire'`
      - Soubor: odeslat příkaz `'file'`, pak hashe všech bloků, pak `'endf'`
   e. Odeslat příkaz `'done'`
2. Jinak (původní chování):
   - Zpracovat jako dosud (jeden soubor)

### Fáze retrieve

1. Pokud `--recursive`:
   a. Vytvořit prázdnou množinu `seen_paths` a seznam `to_delete`
   b. Číst příkazy ze stdin (checksum výstup)
   c. Pro každý `'file'` z dest:
      - Přidat cestu do `seen_paths`
      - Zkontrolovat on-demand, jestli soubor existuje na source
      - Pokud existuje: porovnat hashe, poslat `'file'` + změněné data bloky + `'meta'`
      - Pokud neexistuje na source: přidat cestu do `to_delete`
   d. Pro každý `'dire'` z dest:
      - Přidat cestu do `seen_paths`
      - Zkontrolovat on-demand, jestli adresář existuje na source
      - Pokud existuje: poslat `'dire'` + `'dmet'`
      - Pokud neexistuje na source: přidat cestu do `to_delete`
   e. Po `'done'` ze stdin:
      - Projít source adresář (`followlinks=True`)
      - Pro každou položku která není v `seen_paths`: poslat jako novou
      - Pokud `--delete`: poslat `'remv'` pro položky v `to_delete`
   f. Odeslat `'done'`
2. Jinak (původní chování):
   - Zpracovat jako dosud

### Fáze save

1. Pokud `--recursive`:
   a. Vytvořit prázdný seznam `pending_dir_metadata`
   b. Zpracovat příkazy ze stdin:
      - Pro každý `'dire'`:
        - Vytvořit adresář (pokud neexistuje)
        - Uložit metadata z `'dmet'` do `pending_dir_metadata`
      - Pro každý `'file'`:
        - Otevřít/vytvořit soubor
        - Prealokovat místo pomocí `fallocate()` (pokud je známa velikost z `'meta'`)
        - Zapsat všechny přijaté `'data'` bloky
        - Truncate na velikost z `'meta'.total_size`
        - Aplikovat metadata z `'meta'`
      - Pro každý `'remv'`:
        - Smazat soubor nebo adresář (rekurzivně)
   c. Po `'done'`:
      - Aplikovat metadata adresářů z `pending_dir_metadata` (v opačném pořadí - potomci před rodiči)
   d. Skončit
2. Jinak (původní chování):
   - Zpracovat jako dosud

**Poznámka:** Metadata adresářů (zejména mtime) se aplikují až na konci, protože vytváření souborů uvnitř adresáře by změnilo jeho mtime.

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

### Neexistující dest adresář

Pokud cílový adresář neexistuje nebo je prázdný:
- Checksum pošle `'dire'` s prázdnou cestou (root) a pak `'done'`
- Retrieve pošle všechny položky ze source jako nové
- Save vytvoří celou strukturu od začátku

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
2. Poslat příkaz `file` nebo `dire` pro novou položku (s daty/metadaty)

### Soubory s nulovou velikostí

- Přenášet jako běžné soubory bez hashů
- `file` (checksum) bude mít `file_size = 0`
- Nepošlou se žádné `Hash` příkazy
- V retrieve se pošle `file` + `meta` (bez data bloků)

### Oprávnění a přístup

- Pokud nelze přečíst soubor (PermissionError), vypsat varování na stderr a přeskočit
- Volba `--ignore-errors` pro pokračování i při chybách (budoucí rozšíření)

### Speciální soubory

Device nodes, sockets, named pipes (FIFOs) a další speciální soubory:
- Ignorovat (nepřenášet)
- Vypsat varování na stderr

### Hardlinky

Hardlinky na stejný inode:
- Přenést jako samostatné soubory (každý hardlink se stane nezávislou kopií)
- Pokusit se hardlinky detekovat (sledovat inode) a vypsat varování na stderr
- Budoucí rozšíření může přidat podporu pro zachování hardlinků

### Nevalidní UTF-8 v cestách

Některé filesystémy (např. Linux ext4) povolují libovolné byty v názvech souborů:
- Soubory s nevalidním UTF-8 v cestě přeskočit
- Vypsat varování na stderr

### Sparse soubory

Sparse soubory (soubory s "děrami"):
- Přenášejí se jako plné soubory (nuly se přenesou jako data)
- Budoucí rozšíření může přidat podporu pro detekci a zachování sparse regionů

### Pořadí vytváření

Adresáře se musí vytvářet před soubory, které obsahují:

- Položky seřadit tak, aby rodiče předcházeli potomkům
- Prakticky: seřadit podle cesty zajistí správné pořadí (`a` před `a/b.txt`)

### Pořadí mazání (--delete)

Soubory a adresáře mazat v opačném pořadí (potomci před rodiči):

- Příkazy `remv` seřadit sestupně podle hloubky cesty
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

def send_file_header(out: IO, path: str, size: int):
    """
    Odešle příkaz 'file' (checksum → retrieve, s velikostí).
    """

def send_file_header_retrieve(out: IO, path: str):
    """
    Odešle příkaz 'file' (retrieve → save, bez velikosti).
    """

def send_end_file(out: IO):
    """
    Odešle příkaz 'endf'.
    """

def send_dire_header(out: IO, path: str):
    """
    Odešle příkaz 'dire' (pro oba směry - stejný formát).
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

1. Rozšířit `main_retrieve()` o větev pro `--recursive`
2. Implementovat on-demand kontrolu existence souborů/adresářů na source
3. Implementovat porovnávání hashů a odesílání změněných bloků
4. Implementovat generování příkazů `remv` pro smazání (s --delete)
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

- Množina `seen_paths` se ukládá do paměti (pro detekci nových položek na source)
- Seznam `to_delete` se ukládá do paměti (pro mazání položek chybějících na source)
- Odhad: 1M souborů ≈ 100-200 MB RAM pro cesty

### I/O optimalizace

- Soubory číst sekvenčně (lepší pro HDD)
- Zachovat stávající buffer velikosti (128 KB bloky)
- Pro SSD/NVMe by šlo zvýšit paralelizaci čtení souborů

### Síťová optimalizace

- LZMA komprese výrazně sníží přenos dat
- Zvážit volbu `--compress-level` pro LZMA

## Designová rozhodnutí

Tato sekce dokumentuje klíčová rozhodnutí učiněná během návrhu a jejich zdůvodnění.

### Oddělené příkazy `file` a `dire` místo obecného `entr`

Původní návrh používal obecný příkaz `entr` (entry) s polem typu. Oddělené příkazy jsou:
- Čitelnější v dokumentaci i při debugování
- Jednodušší na parsování (není potřeba switch na typ)
- Explicitnější o tom, co následuje (hashe pro soubory, nic pro adresáře)

### Žádné flags v protokolu retrieve → save

Původní návrh obsahoval FLAG_NEW, FLAG_MODIFIED, FLAG_UNCHANGED pro indikaci akce. Tyto flags byly odstraněny, protože save může inferovat akci z přijatých dat:
- Přišla `data` bloky → zapsat na dané pozice
- Přišlo jen `meta` bez `data` → soubor beze změny, pouze aktualizovat metadata
- Přišlo `remv` → smazat položku

Toto zjednodušuje protokol a eliminuje redundanci.

### Umístění `--delete` na retrieve (ne na save)

Volba `--delete` určuje, zda se mají mazat položky chybějící na source. Retrieve je správné místo, protože:
- Retrieve zná stav source adresáře
- Retrieve generuje příkazy `remv` pro položky k smazání
- Save pouze vykonává přijaté příkazy

### On-demand kontrola source místo manifestu

Původní návrh načítal celý manifest source adresáře do paměti. On-demand přístup:
- Kontroluje existenci souborů/adresářů na source při zpracování každé položky z dest
- Po zpracování všech položek z dest projde source a najde nové položky
- Výhoda: nižší paměťové nároky pro velké adresáře
- Nevýhoda: více syscalls (stat pro každou položku)

### Checksum posílá `dire` pro adresáře

I když adresáře nemají hashe, checksum posílá `dire` příkazy protože:
- Umožňuje detekci prázdných adresářů (které by jinak nebyly vidět)
- Umožňuje detekci adresářů, které na source neexistují (pro `--delete`)
- Retrieve potřebuje vědět o všech položkách na dest

### Symlinky se následují (followlinks=True)

Symlinky se nepřenášejí jako symlinky, ale jako jejich cílový obsah:
- Jednodušší protokol (není potřeba typ SYMLINK)
- Destination dostane skutečná data, ne závislost na lokálních cestách source
- Konzistentní chování nezávislé na umístění source
- Budoucí rozšíření `--no-dereference` může přidat podporu pro kopírování symlinků

### Rozlišení recursive vs. single-file režimu

Receiver může rozlišit režim podle prvního příkazu:
- `'dire'` → recursive režim (první příkaz je vždy root adresář s `path_len=0`)
- `'Hash'` → single-file režim

Toto umožňuje:
- Auto-detekci režimu bez nutnosti explicitního flags
- Root adresář je legitimní položka - jeho metadata se také přenesou
- I prázdný dest adresář pošle `'dire'` s prázdnou cestou, takže recursive režim je vždy rozpoznatelný

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
10. Podpora hardlinků (zachování hardlink vztahů)

## Příloha: Filesystémové funkce

Tato sekce popisuje pokročilé filesystémové funkce, které blockcopy zatím nepodporuje, ale mohou být přidány v budoucnu.

### Rozšířené atributy (xattr)

Rozšířené atributy jsou páry klíč-hodnota připojené k souborům a adresářům mimo standardní metadata (permissions, timestamps).

**Příklady použití:**
- SELinux security labels (`security.selinux`)
- Capabilities (`security.capability`)
- Uživatelská metadata (`user.*`)
- POSIX ACL uložené jako xattr (`system.posix_acl_access`)

**Ukázka práce s xattr:**
```bash
# Nastavení xattr
setfattr -n user.comment -v "důležitý soubor" soubor.txt

# Čtení xattr
getfattr -n user.comment soubor.txt
# soubor.txt: user.comment="důležitý soubor"

# Seznam všech xattr
getfattr -d soubor.txt

# SELinux context
ls -Z soubor.txt
# -rw-r--r--. user user unconfined_u:object_r:user_home_t:s0 soubor.txt
```

**Python API:**
```python
import os

# Čtení
value = os.getxattr('/path/to/file', 'user.comment')

# Zápis
os.setxattr('/path/to/file', 'user.comment', b'hodnota')

# Seznam
attrs = os.listxattr('/path/to/file')
```

**Use cases pro blockcopy:**
- Zachování SELinux kontextu při kopírování mezi servery se SELinux
- Přenos uživatelských metadat (tagy, komentáře)
- Zachování capabilities pro executable soubory

### Access Control Lists (ACL)

ACL rozšiřují standardní UNIX permissions (owner/group/other) o možnost nastavit oprávnění pro libovolné uživatele a skupiny.

**Ukázka práce s ACL:**
```bash
# Nastavení ACL - uživatel alice má právo číst
setfacl -m u:alice:r-- soubor.txt

# Nastavení ACL - skupina devs má právo číst a zapisovat
setfacl -m g:devs:rw- soubor.txt

# Zobrazení ACL
getfacl soubor.txt
# # file: soubor.txt
# # owner: bob
# # group: users
# user::rw-
# user:alice:r--
# group::r--
# group:devs:rw-
# mask::rw-
# other::r--

# Default ACL pro adresář (dědí se na nové soubory)
setfacl -d -m g:devs:rw- adresar/
```

**Python API:**
```python
import posix1e  # pylibacl

# Čtení ACL
acl = posix1e.ACL(file='/path/to/file')
print(acl)

# Kopírování ACL
acl.applyto('/path/to/dest')
```

**Use cases pro blockcopy:**
- Sdílené adresáře s komplexními oprávněními (více týmů, různé úrovně přístupu)
- Webové servery s oddělením oprávnění pro různé aplikace
- Zachování přístupu pro service accounts

### XFS Project Quotas

Project quotas umožňují nastavit diskové kvóty na libovolné adresářové stromy, nezávisle na uživatelích a skupinách.

**Co je "project":**
Project je číselný identifikátor (project ID) přiřazený adresářům a souborům. Všechny soubory se stejným project ID sdílejí společnou kvótu. Na rozdíl od user/group quotas není project vázán na existujícího uživatele.

**Ukázka konfigurace:**
```bash
# /etc/projid - mapování názvů na project ID
webapp:100
logs:101
backups:102

# /etc/projects - mapování project ID na adresáře
100:/var/www/webapp
101:/var/log/webapp
102:/backup/webapp

# Inicializace project quota na adresáři
xfs_quota -x -c 'project -s webapp' /mount

# Nastavení kvóty (soft 8GB, hard 10GB)
xfs_quota -x -c 'limit -p bsoft=8g bhard=10g webapp' /mount

# Zobrazení využití
xfs_quota -x -c 'report -p' /mount
# Project ID       Used   Soft   Hard Warn/Grace
# ---------- ---------- ---------- ---------- ------------
# webapp       2048000  8388608 10485760  00 [--------]
```

**Atribut na souboru:**
```bash
# Zobrazení project ID souboru
xfs_io -c 'lsattr -p' /var/www/webapp/index.html
# 100 /var/www/webapp/index.html

# Nastavení project ID
xfs_io -c 'chattr -p 100' /var/www/webapp/newfile.txt
```

**Proč nepřenášíme:**
- Project ID jsou specifické pro konkrétní server a jeho konfiguraci
- Destination server má typicky jiné project ID mapování
- Kvóty se nastavují na úrovni administrace, ne při kopírování dat

### XFS Reflinks (Copy-on-Write)

Reflinks umožňují vytvořit "lehkou kopii" souboru sdílením datových bloků. Bloky se skutečně zkopírují až při modifikaci (Copy-on-Write).

**Ukázka:**
```bash
# Vytvoření reflink kopie (okamžité, bez kopírování dat)
cp --reflink=always source.img dest.img

# Nebo pomocí xfs_io
xfs_io -c 'reflink source.img' dest.img
```

**Proč není relevantní pro blockcopy:**
- Blockcopy je primárně pro síťový přenos mezi různými servery
- Reflinks fungují pouze v rámci jednoho filesystému
- Pro lokální kopírování na stejném XFS je vhodnější `cp --reflink` nebo rsync
- Blockcopy přenáší data přes síť, takže reflink optimalizace není aplikovatelná

### Preallocation (fallocate)

`fallocate()` umožňuje předem alokovat místo pro soubor bez nutnosti zapisovat data. To může zrychlit zápis velkých souborů.

**Použití v blockcopy:**
Fáze save může před zápisem souboru prealokovat místo pomocí `fallocate()`:

```python
import os

def preallocate_file(fd, size):
    """Prealokuje místo pro soubor."""
    try:
        os.posix_fallocate(fd, 0, size)
    except OSError:
        # Fallback pro filesystémy bez podpory fallocate
        pass
```

**Výhody:**
- Rychlejší zápis (filesystem nemusí alokovat bloky během zápisu)
- Menší fragmentace souborů
- Včasné zjištění nedostatku místa (před zahájením přenosu dat)

**Poznámka:** Toto bude implementováno jako součást --recursive.
