use beluga_core::beluga::{BelFileType, Beluga, Metadata, EXT_RAW_ENTRY};
use pbr::ProgressBar;
use rusqlite::{params, Connection};
use std::vec;

const ENTRY_TABLE: &str = "entry";
const TOKEN_TABLE: &str = "token";

#[derive(Debug)]
struct Entry {
    name: String,
    text: Option<String>,
    binary: Option<Vec<u8>>,
}

struct Token {
    name: String,
    entries: Vec<String>,
}

pub struct RawDict {
    file_type: BelFileType,
    conn: Connection,
    entry_cache: Vec<Entry>,
    token_cache: Vec<Token>,
    cache_size: usize,
}

impl RawDict {
    pub fn new(filepath: &str) -> Self {
        let file_type = if filepath.ends_with(EXT_RAW_ENTRY) {
            BelFileType::Entry
        } else {
            BelFileType::Resource
        };
        let conn = Connection::open(filepath).unwrap();
        conn.execute_batch(
            format!(
                "DROP TABLE IF EXISTS {};
                CREATE TABLE {} (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                name   TEXT UNIQUE,
                text   TEXT,
                binary BLOB
            );
            CREATE INDEX entry_name ON {} (
                name
            );
            ",
                ENTRY_TABLE, ENTRY_TABLE, ENTRY_TABLE
            )
            .as_str(),
        )
        .unwrap();
        conn.execute_batch(
            format!(
                "DROP TABLE IF EXISTS {};
                CREATE TABLE {} (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    name    TEXT    UNIQUE
                                    NOT NULL,
                    entries TEXT
                );
                CREATE INDEX token_name ON {} (
                    name
                );
                ",
                TOKEN_TABLE, TOKEN_TABLE, TOKEN_TABLE
            )
            .as_str(),
        )
        .unwrap();
        Self {
            file_type,
            conn,
            entry_cache: vec![],
            token_cache: vec![],
            cache_size: 200,
        }
    }

    pub fn from(filepath: &str) -> Self {
        let file_type = if filepath.ends_with(EXT_RAW_ENTRY) {
            BelFileType::Entry
        } else {
            BelFileType::Resource
        };
        let conn = Connection::open(filepath).unwrap();
        Self {
            file_type,
            conn,
            entry_cache: vec![],
            token_cache: vec![],
            cache_size: 200,
        }
    }

    pub fn total_entries(&self) -> u64 {
        let mut stmt = self
            .conn
            .prepare(format!("SELECT count(*) as total from {}", ENTRY_TABLE).as_str())
            .unwrap();
        let mut rows = stmt.query(params![]).unwrap();
        let row = rows.next().unwrap().unwrap();
        row.get(0).unwrap()
    }

    pub fn total_tokens(&self) -> u64 {
        let mut stmt = self
            .conn
            .prepare(format!("SELECT count(*) as total FROM {}", TOKEN_TABLE).as_str())
            .unwrap();
        let mut rows = stmt.query(params![]).unwrap();
        let row = rows.next().unwrap().unwrap();
        row.get(0).unwrap()
    }

    pub fn flush_entry_cache(&mut self) {
        let field = if self.file_type == BelFileType::Entry {
            "text"
        } else {
            "binary"
        };
        let tx = self.conn.transaction().unwrap();
        let sql = format!(
            "INSERT INTO {} (name, {}) VALUES ($1, $2)",
            ENTRY_TABLE, field
        );
        {
            let mut stmt = tx.prepare(sql.as_str()).unwrap();
            for wd in &self.entry_cache {
                if field == "text" {
                    if let Err(e) = stmt.execute(params![wd.name, wd.text]) {
                        eprintln!("fail to insert: {}\n {}", wd.name, e);
                    }
                } else {
                    if let Err(e) = stmt.execute(params![wd.name, wd.binary]) {
                        eprintln!("fail to insert: {}\n {}", wd.name, e);
                    }
                }
            }
        }
        tx.commit().unwrap();
        self.entry_cache.clear();
    }

    pub fn flush_token_cache(&mut self) {
        let tx = self.conn.transaction().unwrap();
        let sql = format!(
            "INSERT INTO {} (name, entries) VALUES ($1, $2)",
            TOKEN_TABLE
        );
        {
            let mut stmt = tx.prepare(sql.as_str()).unwrap();
            for item in &self.token_cache {
                stmt.execute(params![
                    item.name,
                    serde_json::to_string(&item.entries).unwrap()
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
        self.token_cache.clear();
    }

    pub fn insert_entry(&mut self, name: &str, value: &[u8]) {
        if self.file_type == BelFileType::Entry {
            self.entry_cache.push(Entry {
                name: String::from(name),
                text: Some(String::from_utf8(value.to_vec()).unwrap()),
                binary: None,
            });
        } else {
            self.entry_cache.push(Entry {
                name: String::from(name),
                text: None,
                binary: Some(value.to_vec()),
            });
        }
        if self.entry_cache.len() >= self.cache_size {
            self.flush_entry_cache();
        }
    }

    pub fn insert_token(&mut self, name: &str, value: &[u8]) {
        let entries = Beluga::parse_token_entries(value);
        self.token_cache.push(Token {
            name: name.to_string(),
            entries,
        });
        if self.token_cache.len() >= self.cache_size {
            self.flush_token_cache();
        }
    }

    pub async fn to_beluga(&self, dest: &str) {
        let meta = Metadata::new();
        let mut dict = Beluga::new(meta, self.file_type);
        let mut id = 0;
        let limit = 100;
        println!("Transformating entry table...");
        let mut bar = ProgressBar::new(self.total_entries());
        loop {
            let mut stmt = self
                .conn
                .prepare(
                    format!(
                        "SELECT * FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
                        ENTRY_TABLE
                    )
                    .as_str(),
                )
                .unwrap();
            let mut list = stmt.query(params![id, limit]).unwrap();
            let mut rows: Vec<Entry> = Vec::new();
            while let Ok(Some(row)) = list.next() {
                id = row.get(0).unwrap();
                rows.push(Entry {
                    name: row.get(1).unwrap(),
                    text: row.get(2).unwrap(),
                    binary: row.get(3).unwrap(),
                })
            }
            let count = rows.len();
            for word in rows {
                let value = match self.file_type {
                    BelFileType::Entry => word.text.unwrap().as_bytes().to_vec(),
                    BelFileType::Resource => word.binary.unwrap(),
                };
                dict.input_entry(word.name, value);
                bar.inc();
            }
            if count < limit {
                break;
            }
        }
        bar.finish();
        let token_num = self.total_tokens();
        if token_num > 0 {
            id = 0;
            println!("Transformating token table...");
            bar = ProgressBar::new(token_num);
            loop {
                let mut stmt = self
                    .conn
                    .prepare(
                        format!(
                            "SELECT * FROM {} WHERE id > $1 ORDER BY id ASC LIMIT $2",
                            TOKEN_TABLE
                        )
                        .as_str(),
                    )
                    .unwrap();
                let mut list = stmt.query(params![id, limit]).unwrap();
                let mut rows: Vec<Token> = Vec::new();
                while let Ok(Some(row)) = list.next() {
                    id = row.get(0).unwrap();
                    let json: String = row.get(2).unwrap();
                    let entries: Vec<String> = serde_json::from_slice(json.as_bytes()).unwrap();
                    rows.push(Token {
                        name: row.get(1).unwrap(),
                        entries,
                    })
                }
                let count = rows.len();
                for row in rows {
                    dict.input_token(row.name, row.entries);
                    bar.inc();
                }
                if count < limit {
                    break;
                }
            }
            bar.finish();
        }
        dict.save(dest);
    }
}
