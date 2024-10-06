# Manufacture

Beluga file is compressed, it is hard to edit, so we provide a database(sqlite) format for editing.

- `.bel-db` is raw of `.bel`
- `.beld-db` is raw of `.beld`

```sql
CREATE TABLE {} (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name   TEXT UNIQUE,
    text   TEXT,
    binary BLOB
)
```

## Data Processing

[beluga-project](https://github.com/youginil/beluga-project) provides a few of useful scripts to process beluga database, fix HTML tag erros, modify HTML structure, tokenization, entry preview...

## File Transformation

- [beluga-builder](https://github.com/youginil/beluga-project/releases) is a file convertor. It can do:
  - `.bel` <--> `.bel-db`
  - `.beld` <--> `.beld-db`
- [Converting Script](https://github.com/youginil/beluga-project) script for converting other dictionary formats to beluga
