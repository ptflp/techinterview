# Структура страниц PostgreSQL (Heap Pages)

## 0) Файл таблицы
Файл = массив страниц по 8 КиБ. Когда переполняется — добавляется ещё 8 КиБ и т.д.

```
┌──────────┬──────────┬─────── … ───────┬──────────┐
│ 8KiB pg  │ 8KiB pg  │                │ 8KiB pg  │
└──────────┴──────────┴─────────────────┴──────────┘
```

## 1) Страница (8 КиБ)
Внутри одной страницы (heap page):

```
offset 0
┌───────────────────────────────────────────────────────────────┐
│ PageHeaderData (фиксированный заголовок, 24 байта)           │
├───────────────────────────────────────────────────────────────┤
│ ItemIdData[0] (4B)  ┐                                         │
│ ItemIdData[1] (4B)  │  <-- массив line pointers               │
│ …                    ┘  (их кол-во = (pd_lower - 24) / 4)     │
├───────────────────────────────────────────────────────────────┤
│                           FREE SPACE                          │
│                (между pd_lower и pd_upper)                    │
├───────────────────────────────────────────────────────────────┤
│ … tuple … | … tuple … | … tuple … | … tuple …  (растут вверх)│
│ ^   ^                                                     ^   │
│ |   └──— смещения/длины на них в ItemIdData                |  │
│ └──— начало области кортежей = pd_upper                     | │
└───────────────────────────────────────────────────────────────┘
                                                             8KiB
```

### 1.1 PageHeaderData (24 байта)
(имена как в исходниках)

```
struct PageHeaderData {
  PageXLogRecPtr pd_lsn;        // 8B: {xlogid u32, xrecoff u32}
  uint16         pd_checksum;   // 2B
  uint16         pd_flags;      // 2B
  LocationIndex  pd_lower;      // 2B : нижняя граница "головы" (ItemIdData)
  LocationIndex  pd_upper;      // 2B : верхняя граница "хвоста" (tuples)
  LocationIndex  pd_special;    // 2B : начало special space (у heap обычно BLCKSZ)
  uint16         pd_pagesize_version; // 2B
  TransactionId  pd_prune_xid;  // 4B : для HOT/prune
}; // всего 24B
```

### Важные инварианты страницы
- `ItemIdData` растут **вниз** от `24` до `pd_lower`.  
- Кортежи растут **вверх** от конца страницы к `pd_upper`.  
- Свободное место: `[pd_lower, pd_upper)`.  
- Для heap-страницы `pd_special = BLCKSZ (8192)` — то есть «special» нет.

### 1.2 ItemIdData (line pointer, 4 байта на запись)
Каждый элемент указывает на один кортеж (либо имеет другое состояние). На диске это два `uint16` со «сквозными» битами флагов:

```
raw (LE): [lp_off:16] [lp_len:16]
Но реально:
  lp_flags — 2 бита «расползлись» по старш.биту lp_off и младш.биту lp_len
  lp_off   — 15 бит (смещение кортежа от начала страницы)
  lp_len   — 15 бит (длина кортежа)
  lp_flags:
    0 = LP_UNUSED
    1 = LP_NORMAL
    2 = LP_REDIRECT (HOT redirect)
    3 = LP_DEAD
```

Практически тебе нужны:
- **offset** кортежа: `lp_off & 0x7FFF`
- **length** кортежа: `lp_len >> 1`
- **flags**: `((lp_off>>15)&1) | ((lp_len<<1)&2)`

## 2) Кортеж (tuple) в «хвосте» страницы
Каждый кортеж = **заголовок** + **пост-заголовок (опц.)** + **данные атрибутов**. Смещение к началу кортежа берём из `ItemIdData[i].lp_off`.

```
tuple @ lp_off:
┌──────────────────────────────┬───────────────────────────┬─────────────┐
│ HeapTupleHeaderData (~23 B)  │  NULL bitmap?  OID? PAD? │  DATA AREA  │
└──────────────────────────────┴───────────────────────────┴─────────────┘
                                 ↑ t_hoff указывает на старт DATA AREA
```

### 2.1 HeapTupleHeaderData (минимально ~23 байта)
Состав (упрощённо; порядок полей как в PG):

```
struct HeapTupleHeaderData {
  // HeapTupleFields:
  TransactionId t_xmin;      // 4B
  TransactionId t_xmax;      // 4B
  union {                    // 4B
    CommandId    t_cid;
    TransactionId t_xvac;
  } t_field3;

  // TID self-pointer (ctid):
  // ItemPointerData = BlockIdData (2B hi, 2B lo) + OffsetNumber (2B)
  BlockIdData   t_ctid_block;   // 4B
  uint16        t_ctid_offset;  // 2B

  uint16        t_infomask2;    // 2B (низк. 11 бит = кол-во атрибутов)
  uint16        t_infomask;     // 2B (флаги видимости, NULLs, OID и пр.)
  uint8         t_hoff;         // 1B  (смещение начала DATA AREA)
}; // Итого 23 байта (часто +1B выравнивание до t_hoff)
```

Что может идти между заголовком и DATA:
- **NULL bitmap** (если есть nullable-колонки): `⌈natts/8⌉` байт, отмечает какие атрибуты `NULL`.  
- **OID** (редко, если включено на таблице).  
- **Паддинг выравнивания** до `t_hoff` (MAXALIGN).  
`t_hoff` всегда указывает на первый байт **первого атрибута** (DATA AREA).

### 2.2 DATA AREA (значения атрибутов по порядку колонок)
Идут подряд, **с учётом выравнивания каждого типа**:
- Фикс-длина (`int4`, `int8`, `bool`, `timestamp` …): кладутся напрямую; начало каждого значения выравнивается по `attalign` (`c`=1, `s`=2, `i`=4, `d`=8).
- Переменная длина (**varlena**: `text`, `bytea`, `jsonb`, `varchar` …):
  - «Длинная» форма: 4-байтовая длина+флаги + данные.
  - «Короткая» форма: 1 байт длины+флаги + данные (до 126 байт полезной длины).
  - На LE платформах последний бит первого байта = 1 для короткой формы (и длина «включает себя», см. сдвиги/маски).

Если у колонки в NULL-bitmap стоит `NULL`, **значение в DATA для неё не пишется** — парсишь, пропуская места только у ненулевых.

## 3) Быстрый «путь чтения» по смещениям
1) Читаешь страницу (8192 байта) → `PageHeaderData`.  
2) Считаешь `nItemIds = (pd_lower - sizeof(PageHeaderData)) / 4`.  
3) Для каждого `i ∈ [0..nItemIds)`:
   - читаешь `ItemIdData[i]`;
   - если `lp_flags == LP_NORMAL`:
     - `tuple := page[lp_off : lp_off + lp_len]`
     - из начала `tuple` читаешь `HeapTupleHeaderData`
     - `dataStart := t_hoff`
     - если в `t_infomask` проставлено `HEAP_HASNULL` — читаешь NULL-bitmap и учитываешь её при проходе по атрибутам
     - проходишь по атрибутам в порядке `pg_attribute.attnum`, с учётом `attlen/attalign` и **varlena-правил** (и возможного `TOAST`, если «настоящих» данных нет, а лежит указатель).

## 4) Мини-шпаргалка битов/полей
- `t_infomask2` (aka `InfoMask2` у тебя):  
  - низк. 11 бит = `HEAP_NATTS_MASK` (число атрибутов),  
  - биты: `HEAP_HOT_UPDATED`, `HEAP_ONLY_TUPLE`, `HEAP_KEYS_UPDATED` и т.п.
- `t_infomask`: флаги видимости/состояния (например, `HEAP_XMIN_COMMITTED`, `HEAP_XMAX_INVALID`, `HEAP_HASNULL`, `HEAP_HASVARWIDTH`, `HEAP_HASOID` и др.).
- `t_ctid`: «самоуказатель» кортежа (Block, Offset). При UPDATE у старой версии `ctid` указывает на новую («chain»).
- `pd_prune_xid`: минимальный XID, нужный prune’у для HOT-цепочек.
- **Free space** на странице: `pd_upper - pd_lower`.

## 5) Схемка «вложенности» целиком

```
Файл таблицы
└── Страница (8KiB)
    ├── PageHeaderData (24B)
    ├── ItemIdData[0..N-1] (каждый 4B)
    ├── FREE SPACE (между pd_lower и pd_upper)
    └── Tuples (каждый по ItemIdData[i].lp_off / lp_len, растут вверх)
        └── HeapTupleHeaderData (~23B)
            ├── t_xmin, t_xmax, t_cid/t_xvac
            ├── t_ctid (BlockIdHi, BlockIdLo, OffsetNumber)
            ├── t_infomask2 (natts + флаги)
            ├── t_infomask  (флаги)
            ├── t_hoff
            ├── [NULL bitmap, если HEAP_HASNULL]
            ├── [OID, если HEAP_HASOID]
            ├── [padding до t_hoff]
            └── DATA AREA (атрибуты по порядку)
                ├── attr1 (align по attalign, фикс/varlena)
                ├── attr2
                └── …
```

## 6) Маленькая проверка на «здоровье» страницы
- `24 ≤ pd_lower ≤ pd_upper ≤ pd_special ≤ 8192`
- `pd_special == 8192` у heap-страницы
- для каждого `ItemIdData` c `LP_NORMAL`:  
  `0 < lp_off < 8192`,  
  `lp_off + lp_len ≤ pd_special`,  
  `lp_off ≥ pd_upper`.
