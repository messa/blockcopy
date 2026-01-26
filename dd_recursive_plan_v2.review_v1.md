# Review dokumentu add_recursive_plan_v2.md

## Celkové hodnocení

**Bude to fungovat?** Ano, po opravě kritických bodů níže.

**Je to správně navržené?** Ano, přístup je principiálně správný a podobný rsync. Pro uvedené use cases (VM images, databáze) je fixed-block hashing vhodný.

---

## Kritické problémy

### 1. Nekonzistence umístění `--delete`

Dokument si protiřečí:
- **Řádek 301-303** ukazuje: `blockcopy save --recursive --delete /path/to/dest_dir`
- **Řádek 773-779** argumentuje, že `--delete` patří na retrieve

Jedno z toho je špatně. Podle logiky v sekci "Designová rozhodnutí" by mělo být `--delete` na retrieve (protože retrieve generuje `remv` příkazy). CLI příklad na řádku 301-303 je tedy chybný.

### 2. Změna typu položky - chyba v algoritmu

Sekce "Změna typu položky" (řádky 344-349) popisuje co se má stát, ale algoritmus retrieve (řádky 242-259) to neimplementuje správně:

**Problém:** Pokud dest má soubor `foo` a source má adresář `foo`:
1. Checksum pošle `file + "foo"`
2. Retrieve přidá `"foo"` do `seen_paths`
3. Retrieve zjistí, že `foo` na source není soubor → přidá do `to_delete` ✓
4. Retrieve projde source, najde adresář `foo`
5. `"foo"` JE v `seen_paths` → nepošle se jako nový ✗

**Řešení:** Buď rozlišit `seen_files` a `seen_dirs`, nebo přidávat do `seen_paths` pouze pokud položka existuje na source se stejným typem.

---

## Střední problémy

### 3. Symlink cykly

Dokument zmiňuje `followlinks=True`, ale neřeší detekci cyklů. Symlink `/a/b/c → /a` způsobí nekonečnou smyčku. Python's `os.walk` s `followlinks=True` může zacyklit.

**Řešení:** Sledovat navštívené inodes (device, inode) a přeskočit již navštívené adresáře.

### 4. Metadata pro nezměněné soubory

Řádky 199-201 ukazují, že i pro nezměněný soubor se posílá `meta`:
```
file + "src/utils/helper.py"
  meta + <metadata>
endf
```

Je to záměr? Výhoda: umožňuje aktualizovat permissions/timestamps i bez změny obsahu. Nevýhoda: bandwidth overhead. Mělo by být explicitně zdokumentováno, proč se to dělá.

---

## Drobné poznámky

### 5. Pořadí není striktně abecední

Řádek 13 říká "položky se zpracovávají v abecedním pořadí", ale skutečné pořadí výstupu retrieve je:
1. Odpovědi na položky z dest (abecedně mezi sebou)
2. Nové položky ze source (abecedně mezi sebou)
3. Příkazy `remv`

To není celkově abecední. Pro správné fungování to nevadí (rodiče stále předchází potomkům), ale dokumentace je zavádějící.

### 6. Chybí verzování protokolu

Dokument říká, že změny nejsou zpětně kompatibilní s 0.0.3, ale protokol nemá mechanismus pro vyjednání verze. Auto-detekce podle prvního příkazu (`dire` vs `Hash`) rozliší recursive/single-file, ale neřeší budoucí změny protokolu.

---

## Co opravit před implementací

1. Sjednotit `--delete` na retrieve (opravit CLI příklad)
2. Opravit algoritmus pro změnu typu položky
3. Přidat detekci symlink cyklů do `walk_directory()`
