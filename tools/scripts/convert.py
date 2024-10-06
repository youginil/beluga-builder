import math
import sys
from pathlib import Path
from typing import List
from pyglossary import Glossary, ui_type
import sqlite3
from progressbar import ProgressBar

if len(sys.argv) < 2:
    raise RuntimeError("invalid argument number")

source_file = sys.argv[1]
parent_dir = Path(source_file).parent
stemname = Path(source_file).stem
entry_file = parent_dir.joinpath(stemname + ".bel-db").as_posix()
resource_file = parent_dir.joinpath(stemname + ".beld-db").as_posix()


class Pbr(ui_type.UIType):
    bar: ProgressBar

    def __init__(self) -> None:
        super().__init__()

    def progressInit(self, title: str) -> None:
        self.bar = ProgressBar(max_value=100, prefix=title)

    def progress(self, rat: float, text: str = "") -> None:
        self.bar.update(math.floor(rat * 100.0))

    def progressEnd(self) -> None:
        self.bar.finish()


Glossary.init()
glos = Glossary(ui=Pbr())
if not glos.read(source_file, direct=True):
    raise RuntimeError("fail to read source file")

entry_table = "entry"
token_table = "token"
entry_conn: sqlite3.Connection | None = None
entry_pool: List[tuple[str, str]] = []
resource_conn: sqlite3.Connection | None = None
resource_pool: List[tuple[str, bytes]] = []


def init_db(file: str) -> sqlite3.Connection:
    conn = sqlite3.connect(file)
    conn.execute(f"DROP TABLE IF EXISTS {entry_table}")
    conn.execute(
        f"CREATE TABLE {entry_table} (\
                id     INTEGER PRIMARY KEY AUTOINCREMENT,\
                name   TEXT UNIQUE,\
                text   TEXT,\
                binary BLOB)"
    )
    conn.execute(f"CREATE INDEX entry_name ON {entry_table} (name)")

    conn.execute(f"DROP TABLE IF EXISTS {token_table}")
    conn.execute(
        f"CREATE TABLE {token_table} (\
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,\
                    name    TEXT    UNIQUE\
                                    NOT NULL,\
                    entries TEXT\
                )"
    )
    conn.execute(f"CREATE INDEX token_name ON {token_table} (name)")
    return conn


def flush_entry():
    global entry_pool
    global entry_conn
    if len(entry_pool) == 0:
        return
    if entry_conn is None:
        entry_conn = init_db(entry_file)
    entry_conn.executemany(
        f"INSERT INTO {entry_table}(name, text) VALUES (?, ?) ON CONFLICT (name) DO NOTHING",
        entry_pool,
    )
    entry_conn.commit()
    entry_pool.clear()


def flush_resource():
    global resource_pool
    global resource_conn
    if len(resource_pool) == 0:
        return
    if resource_conn is None:
        resource_conn = init_db(resource_file)
    resource_conn.executemany(
        f"INSERT INTO {entry_table}(name, binary) VALUES (?, ?) ON CONFLICT (name) DO NOTHING",
        resource_pool,
    )
    resource_conn.commit()
    resource_pool.clear()


def insert_entry(name: str, value: str):
    global entry_pool
    entry_pool.append((name, value))
    if len(entry_pool) >= 100:
        flush_entry()


def insert_resource(name: str, value: bytes):
    global resource_pool
    resource_pool.append((name, value))
    if len(resource_pool) >= 100:
        flush_resource()


num_entry = 0
num_resource = 0
for entry in glos:
    if entry.isData():
        insert_resource(entry.s_word, entry.data)
    else:
        insert_entry(entry.s_word, entry.defi)

flush_entry()
flush_resource()
