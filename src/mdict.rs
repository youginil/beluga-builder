use crate::{raw::RawDict, utils::*};
use beluga_core::beluga::{Beluga, BelFileType, Metadata};
use flate2::read::ZlibDecoder;
use pbr::ProgressBar;
use quick_xml::{
    events::{attributes::Attribute, Event},
    Reader,
};
use ripemd128::{Digest, Ripemd128};
use rust_lzo::{LZOContext, LZOError};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::Path;
use std::rc::Rc;

#[derive(Debug)]
struct Summary {
    num_blocks: u64,
    num_entries: u64,
    key_index_decomp_len: u64,
    key_index_comp_len: u64,
    key_blocks_len: u64,
}

#[derive(Debug, Clone)]
struct KeywordIndex {
    num_entries: u64,
    first_word: String,
    last_word: String,
    comp_size: u64,
    decomp_size: u64,
    block_offset: u64,
    block: Vec<Keyword>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Keyword {
    offset: u64,
    key: String,
    size: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct RecordSummary {
    num_blocks: u64,
    num_entries: u64,
    index_len: u64,
    blocks_len: u64,
    blocks_pos: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Definition {
    name: String,
    content: String,
}

pub struct Mdict {
    file: File,
    is_index: bool,
    attrs: HashMap<String, String>,
    v2: bool,
    encrypt: u8,
    utf16: bool,
    summary: Summary,
    kis: Vec<KeywordIndex>,
    record_summary: RecordSummary,
    record_index: Vec<(u64, u64)>,
    cache_offset: u64,
    cache: Rc<Vec<u8>>,
}

impl Mdict {
    pub fn new(p: &str) -> Result<Self, String> {
        let file = match File::open(p) {
            Ok(f) => f,
            Err(_e) => {
                return Err(String::from("Invalid mdict file path"));
            }
        };
        let is_index = match Path::new(p).extension() {
            Some(ext) => match ext.to_str() {
                Some("mdx") => true,
                Some("mdd") => false,
                _ => {
                    return Err(String::from("Invalid mdict extension name"));
                }
            },
            None => {
                return Err(String::from("Invalid mdict extension name"));
            }
        };
        let instance = Self {
            file,
            is_index,
            attrs: HashMap::new(),
            v2: false,
            encrypt: 0,
            utf16: false,
            summary: Summary {
                num_blocks: 0,
                num_entries: 0,
                key_index_decomp_len: 0,
                key_index_comp_len: 0,
                key_blocks_len: 0,
            },
            kis: Vec::new(),
            record_summary: RecordSummary {
                num_blocks: 0,
                num_entries: 0,
                index_len: 0,
                blocks_len: 0,
                blocks_pos: 0,
            },
            record_index: Vec::new(),
            cache_offset: 0,
            cache: Rc::new(Vec::new()),
        };
        Ok(instance)
    }

    fn seek(&mut self, pos: u64) -> Result<(), String> {
        match self.file.seek(SeekFrom::Start(pos)) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn curpos(&mut self) -> Result<u64, String> {
        match self.file.seek(SeekFrom::Current(0)) {
            Ok(n) => Ok(n),
            Err(e) => Err(e.to_string()),
        }
    }

    fn read(&mut self, n: usize) -> Result<Vec<u8>, String> {
        let mut buf: Vec<u8> = vec![0; n];
        match self.file.read(&mut buf) {
            Ok(_) => Ok(buf),
            Err(e) => Err(e.to_string()),
        }
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let buf = self.read(8)?;
        let n = u8v_to_u64(&buf)?;
        Ok(n)
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let buf = self.read(4)?;
        let n = u8v_to_u32(&buf)?;
        Ok(n)
    }

    fn read_number(&mut self) -> Result<u64, String> {
        if self.v2 {
            let n = self.read_u64()?;
            return Ok(n);
        }
        let n = self.read_u32()?;
        Ok(n as u64)
    }

    fn parse<F>(&mut self, cb: F) -> Result<(), String>
    where
        F: Fn(String, Vec<u8>),
    {
        if let Err(e) = self.file.seek(SeekFrom::Start(0)) {
            return Err(e.to_string());
        }
        self.parse_header().unwrap();
        // skip checksum
        if let Err(e) = self.file.seek(SeekFrom::Current(4)) {
            return Err(e.to_string());
        }
        self.parse_summary().unwrap();
        //skip checksum
        if let Err(e) = self.file.seek(SeekFrom::Current(4)) {
            return Err(e.to_string());
        }
        self.parse_keyword_index().unwrap();
        self.parse_keyword_block().unwrap();
        self.parse_record_summary().unwrap();
        self.parse_record_index().unwrap();
        println!(">>> Parsing words");
        // @todo performace problem
        let kis = self.kis.clone();
        let mut pb = ProgressBar::new(self.summary.num_entries);
        for item in kis.iter() {
            for kw in item.block.iter() {
                match self.parse_definition(kw) {
                    Ok((key, data)) => cb(key, data),
                    Err(e) => eprintln!("{}", e),
                }
                pb.inc();
            }
        }
        pb.finish_print("Done");
        Ok(())
    }

    fn parse_header(&mut self) -> Result<(), String> {
        println!(">>> Parsing Header");
        let length = self.read_u32()?;
        let buf = self.read(length as usize)?;
        let buf = u8v_to_u16v(&buf, Endianness::Little)?;
        let content = match String::from_utf16(&buf) {
            Ok(s) => s,
            Err(e) => {
                return Err(e.to_string());
            }
        };

        let mut reader = Reader::from_str(content.as_str());
        reader.trim_text(true);
        loop {
            match reader.read_event() {
                Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                    b"Dictionary" | b"Library_Data" => {
                        for attr in e.attributes() {
                            match attr {
                                Ok(Attribute { key: k, value: v }) => {
                                    let key = match std::str::from_utf8(k.as_ref()) {
                                        Ok(k) => k,
                                        Err(e) => {
                                            return Err(e.to_string());
                                        }
                                    };
                                    let value = match String::from_utf8(v.into_owned()) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            return Err(e.to_string());
                                        }
                                    };
                                    self.attrs.insert(String::from(key), value);
                                }
                                Err(e) => {
                                    return Err(format!("Invalid attribute: {:?}", e));
                                }
                            }
                        }
                    }
                    _ => {
                        println!("{:?}", e)
                    }
                },
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
        }
        let version = match self.attrs.get("GeneratedByEngineVersion") {
            Some(v) => v,
            None => {
                return Err(String::from("No field: GeneratedByEngineVersion"));
            }
        };
        let version = match version.parse::<f32>() {
            Ok(n) => n,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        if version >= 2.0 {
            self.v2 = true;
        }
        let encrypt = match self.attrs.get("Encrypted") {
            Some(s) => s,
            None => {
                return Err(String::from("No field: Encrypted"));
            }
        };
        if encrypt.as_str().to_lowercase() == "no" {
            self.encrypt = 0;
        } else {
            self.encrypt = match encrypt.parse::<u8>() {
                Ok(v) => v,
                Err(e) => {
                    return Err(e.to_string());
                }
            };
        }
        let encoding = match self.attrs.get("Encoding") {
            Some(s) => s.as_str(),
            None => "",
        };
        self.utf16 = encoding == "UTF16" || encoding == "";
        Ok(())
    }

    fn parse_summary(&mut self) -> Result<(), String> {
        println!(">>> Parsing Summary");
        self.summary.num_blocks = self.read_number()?;
        self.summary.num_entries = self.read_number()?;
        if self.v2 {
            self.summary.key_index_decomp_len = self.read_number()?;
        }
        self.summary.key_index_comp_len = self.read_number()?;
        self.summary.key_blocks_len = self.read_number()?;
        println!("{:?}", self.summary);
        Ok(())
    }

    fn parse_keyword_index(&mut self) -> Result<(), String> {
        println!(">>> Parsing Key Index");
        let mut buf = self.read(self.summary.key_index_comp_len as usize)?;
        let buf = read_block(
            &mut buf,
            self.summary.key_index_decomp_len as usize,
            self.encrypt,
        )?;
        let buf = Rc::new(buf);
        let mut scanner = Scanner::new(buf, self.v2, self.utf16);
        let mut block_offset = 0;
        for i in 0..self.summary.num_blocks {
            let num_entries = scanner.read_number()?;
            let first_size = scanner.read_short_number()?;
            let first_word = scanner.read_text(first_size as usize)?;
            let last_size = scanner.read_short_number()?;
            let last_word = scanner.read_text(last_size as usize)?;
            let comp_size = scanner.read_number()?;
            let decomp_size = scanner.read_number()?;
            self.kis.insert(
                i as usize,
                KeywordIndex {
                    num_entries,
                    first_word,
                    last_word,
                    comp_size,
                    decomp_size,
                    block_offset,
                    block: Vec::new(),
                },
            );
            block_offset += comp_size;
        }
        println!("keyword index length is {}", self.kis.len());
        Ok(())
    }

    fn parse_keyword_block(&mut self) -> Result<(), String> {
        println!(">>> Parsing keyword blocks");
        let buf = self.read(self.summary.key_blocks_len as usize)?;
        let buf = Rc::new(buf);
        let mut scanner = Scanner::new(buf, self.v2, self.utf16);
        for item in self.kis.iter_mut() {
            scanner.seek(item.block_offset as usize);
            let mut bf = scanner.read(item.comp_size as usize)?;
            let b = read_block(&mut bf, item.decomp_size as usize, 0)?;
            let b = Rc::new(b);
            let mut bs = Scanner::new(b, self.v2, self.utf16);
            for i in 0..item.num_entries {
                let offset = bs.read_number()?;
                let key = bs.read_text_unsized()?;
                if i > 1 {
                    item.block[(i - 1) as usize].size =
                        offset - item.block[(i - 1) as usize].offset;
                }
                item.block.push(Keyword {
                    offset,
                    key,
                    size: 0,
                });
            }
            println!(
                "block ({} ~ {}) {} words",
                item.first_word,
                item.last_word,
                item.block.len()
            );
        }
        Ok(())
    }

    fn parse_record_summary(&mut self) -> Result<(), String> {
        println!(">>> Paring record summary");
        let buf = self.read(32)?;
        let buf = Rc::new(buf);
        let mut scanner = Scanner::new(buf, self.v2, self.utf16);
        self.record_summary.num_blocks = scanner.read_number()?;
        self.record_summary.num_entries = scanner.read_number()?;
        self.record_summary.index_len = scanner.read_number()?;
        self.record_summary.blocks_len = scanner.read_number()?;
        self.record_summary.blocks_pos = self.curpos()? + self.record_summary.index_len;
        println!("{:?}", self.record_summary);
        Ok(())
    }

    fn parse_record_index(&mut self) -> Result<(), String> {
        println!(">>> Parsing record index");
        let buf = self.read(self.record_summary.index_len as usize)?;
        let buf = Rc::new(buf);
        let mut scanner = Scanner::new(buf, self.v2, self.utf16);
        let mut p0 = self.record_summary.blocks_pos;
        let mut p1: u64 = 0;
        for _ in 0..self.record_summary.num_blocks {
            self.record_index.push((p0, p1));
            p0 += scanner.read_number()?;
            p1 += scanner.read_number()?;
        }
        self.record_index.push((p0, p1));
        Ok(())
    }

    fn parse_definition(&mut self, kw: &Keyword) -> Result<(String, Vec<u8>), String> {
        // println!(">>> Parsing definition of \"{}\"", kw.key);
        if self.record_index.len() == 0 {
            return Err(String::from("Invalid record index length"));
        }
        if kw.offset > self.record_index[self.record_index.len() - 1].1
            || kw.offset < self.record_index[0].1
        {
            return Err(String::from("Out of index of record"));
        }
        let mut hi = self.record_index.len() - 1;
        let mut li: usize = 0;
        let comp_offset: u64;
        let decomp_offset: u64;
        let comp_size: u64;
        let decomp_size: u64;
        loop {
            let mi = (hi + li) / 2;
            let (_, o2) = self.record_index[mi];
            if kw.offset >= o2 {
                li = mi;
            } else {
                hi = mi;
            }
            if hi - li <= 1 {
                let (o1, o2) = self.record_index[li];
                comp_offset = o1;
                decomp_offset = o2;
                let (o1, o2) = self.record_index[li + 1];
                comp_size = o1 - comp_offset;
                decomp_size = o2 - decomp_offset;
                break;
            }
        }
        let mut scanner: Scanner;
        if self.cache_offset == comp_offset && self.cache.len() > 0 {
            // todo performance
            scanner = Scanner::new(self.cache.clone(), self.v2, self.utf16);
        } else {
            self.seek(comp_offset).unwrap();
            let mut buffer = self.read(comp_size as usize)?;
            let buf = read_block(&mut buffer, decomp_size as usize, 0)?;
            let buf = Rc::new(buf);
            self.cache_offset = comp_offset;
            // todo performance
            self.cache = Rc::clone(&buf);
            scanner = Scanner::new(buf, self.v2, self.utf16);
        }
        scanner.forward((kw.offset - decomp_offset) as usize);
        let data: Vec<u8>;
        if self.is_index {
            let txt = scanner.read_text_unsized()?;
            data = txt.as_bytes().to_vec();
        } else {
            let mut size = kw.size as usize;
            // fix last keyword size of block
            if size == 0 {
                size = scanner.buf.len() - scanner.pos;
            }
            data = scanner.read(size as usize)?;
        }
        let key = kw.key.clone();
        Ok((key, data))
    }

    pub async fn to_beluga_index(&mut self, dest: &str) {
        let meta = Metadata::new();
        let dict = RefCell::new(Beluga::new(meta, BelFileType::Entry));
        self.parse(|key, value| {
            dict.borrow_mut().input_entry(key, value);
        })
        .unwrap();
        dict.borrow_mut()
            .save(dest)
            .await
            .expect("fail to convert to beluga");
    }

    pub async fn to_beluga_data(&mut self, dest: &str) {
        let meta = Metadata::new();
        let dict = RefCell::new(Beluga::new(meta, BelFileType::Resource));
        self.parse(|key, value| {
            dict.borrow_mut().input_entry(key, value);
        })
        .unwrap();
        dict.borrow_mut()
            .save(dest)
            .await
            .expect("fail to convert to beluga");
    }

    pub fn to_raw(&mut self, dest: &str) {
        let raw = RefCell::new(RawDict::new(dest));
        self.parse(|key, value| raw.borrow_mut().insert_entry(key.as_str(), &value))
            .unwrap();
        raw.borrow_mut().flush_entry_cache();
    }
}

struct Scanner {
    buf: Rc<Vec<u8>>,
    pos: usize,
    v2: bool,
    utf16: bool,
    text_tail: usize,
}

impl Scanner {
    fn new(buf: Rc<Vec<u8>>, v2: bool, utf16: bool) -> Self {
        let mut text_tail: usize = 0;
        if v2 {
            if utf16 {
                text_tail = 2;
            } else {
                text_tail = 1;
            }
        }
        Self {
            buf,
            pos: 0,
            v2,
            utf16,
            text_tail,
        }
    }

    fn seek(&mut self, pos: usize) {
        self.pos = pos;
    }

    fn forward(&mut self, n: usize) {
        self.pos += n;
    }

    fn read(&mut self, n: usize) -> Result<Vec<u8>, String> {
        if self.pos + n > self.buf.len() {
            return Err(format!(
                "Invalid read size. pos: {}, size: {}, len: {}",
                self.pos,
                n,
                self.buf.len()
            ));
        }
        let mut r: Vec<u8> = Vec::with_capacity(n);
        for i in 0..n {
            r.insert(i, self.buf[self.pos + i]);
        }
        self.pos += n;
        Ok(r)
    }

    fn read_number(&mut self) -> Result<u64, String> {
        if self.v2 {
            let buf = self.read(8)?;
            let n = u8v_to_u64(&buf)?;
            return Ok(n);
        }
        let buf = self.read(4)?;
        let n = u8v_to_u32(&buf)?;
        Ok(n as u64)
    }

    fn read_short_number(&mut self) -> Result<u16, String> {
        if self.v2 {
            let buf = self.read(2)?;
            let n = u8v_to_u16(&buf)?;
            return Ok(n);
        }
        let buf = self.read(1)?;
        Ok(0u16 | (buf[0] as u16))
    }

    /**
     * @todo other Encoding compatible
     */
    fn read_text(&mut self, n: usize) -> Result<String, String> {
        if self.utf16 {
            let buf = self.read(n * 2)?;
            let buf = u8v_to_u16v(&buf, Endianness::Little)?;
            self.forward(self.text_tail);
            return match String::from_utf16(&buf) {
                Ok(s) => Ok(s),
                Err(e) => Err(e.to_string()),
            };
        }
        let buf = self.read(n)?;
        self.forward(self.text_tail);
        match String::from_utf8(buf) {
            Ok(s) => Ok(s),
            Err(e) => Err(e.to_string()),
        }
    }

    fn read_text_unsized(&mut self) -> Result<String, String> {
        let mut length = 0;
        let pos = self.pos;
        if self.utf16 {
            loop {
                let buf = self.read(2)?;
                if u8v_to_u16(&buf)? == 0x0000 {
                    break;
                }
                length += 2;
            }
        } else {
            loop {
                if self.read(1)?[0] == 0x00 {
                    break;
                }
                length += 1;
            }
        }
        self.seek(pos);
        let buf = self.read(length)?;
        if self.utf16 {
            let buf = u8v_to_u16v(&buf, Endianness::Little)?;
            self.forward(2);
            return match String::from_utf16(&buf) {
                Ok(s) => Ok(s),
                Err(e) => Err(e.to_string()),
            };
        }
        self.forward(1);
        match String::from_utf8(buf) {
            Ok(s) => Ok(s),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub fn decrypt(buf: &mut Vec<u8>, key: [u8; 8]) {
    let mut hasher = Ripemd128::new();
    hasher.input(key);
    let k = hasher.result();
    let kl = k.len();
    let mut prev: u8 = 0x36;
    for i in 0..buf.len() {
        let b = buf[i];
        let b = (b >> 4) | (b << 4);
        let b = b ^ prev ^ ((i & 0xFF) as u8) ^ k[i % kl];
        prev = buf[i];
        buf[i] = b;
    }
}

fn read_block(buf: &mut Vec<u8>, decompress_length: usize, encrypt: u8) -> Result<Vec<u8>, String> {
    let compress = buf[0];
    let mut result: Vec<u8>;
    if compress == 0 {
        result = vec![0; buf.len() - 8];
        for (i, &item) in buf[8..].iter().enumerate() {
            result[i] = item;
        }
    } else {
        let tmp: Vec<u8> = buf.drain(0..8).collect();
        if encrypt & 0x02 != 0 {
            let mut passkey: [u8; 8] = [0, 0, 0, 0, 0x95, 0x36, 0x00, 0x00];
            for (i, &item) in tmp[4..8].iter().enumerate() {
                passkey[i] = item;
            }
            decrypt(buf, passkey);
        }
        if compress == 2 {
            let mut d = ZlibDecoder::new(&buf[..]);
            result = Vec::new();
            if let Err(e) = d.read_to_end(&mut result) {
                return Err(e.to_string());
            }
        } else {
            result = Vec::with_capacity(decompress_length);
            let (_, e) = LZOContext::decompress_to_slice(&buf[..], &mut result);
            match e {
                LZOError::OK => {}
                _ => {
                    panic!("LZO decompress error");
                }
            }
        }
    }
    Ok(result)
}
