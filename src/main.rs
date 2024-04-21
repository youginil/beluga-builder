use beluga_core::beluga::*;
use clap::{Arg, Command};
use mdict::*;
use pbr::ProgressBar;
use raw::RawDict;
use std::path::Path;

mod mdict;
mod raw;
mod utils;

#[tokio::main]
async fn main() {
    let matches = Command::new("Beluga Dictionary Builder")
        .version("0.1.3")
        .about("Transform dictionary format. `.mdx` -> `.bel-db`, `.mdd` -> `.beld-db`, `.bel-db` <-> `.bel`, `.beld-db` <->`.beld`")
        .arg(
            Arg::new("input")
                .short('i')
                .num_args(1)
                .value_name("SOURCE")
                .help("Source file")
                .required(true),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .num_args(1)
                .value_name("TARGET")
                .help("Target file")
                .required(true),
        )
        .get_matches();
    let source: &String = matches.get_one("input").expect("no source file");
    let target: &String = matches.get_one("output").expect("no target file");

    let source_ext = match Path::new(source).extension() {
        Some(v) => v.to_str().unwrap(),
        None => panic!("Invalid input file extension"),
    };
    let target_ext = match Path::new(target).extension() {
        Some(v) => v.to_str().unwrap(),
        None => panic!("Invalid target file extension"),
    };

    match (source_ext, target_ext) {
        ("mdx", EXT_ENTRY) => {
            let mut dict = Mdict::new(source).unwrap();
            dict.to_beluga_index(target).await;
        }
        ("mdd", EXT_RESOURCE) => {
            let mut dict = Mdict::new(source).unwrap();
            dict.to_beluga_data(target).await;
        }
        ("mdx", EXT_RAW_ENTRY) | ("mdd", EXT_RAW_RESOURCE) => {
            let mut dict = Mdict::new(source).unwrap();
            dict.to_raw(target);
        }
        (EXT_ENTRY, EXT_RAW_ENTRY) | (EXT_RESOURCE, EXT_RAW_RESOURCE) => {
            let dict = Beluga::from_file(source).await;
            let entry_num = dict.metadata.entry_num;
            let mut bar = ProgressBar::new(entry_num);
            if !((target.ends_with(EXT_RAW_ENTRY) && dict.file_type == BelFileType::Entry)
                || (target.ends_with(EXT_RAW_RESOURCE) && dict.file_type == BelFileType::Resource))
            {
                panic!("Invalid destination filename");
            }
            let mut raw = RawDict::new(target);

            let mut count = 0;
            dict.traverse_entry(&mut |key: &EntryKey, value: &EntryValue| {
                raw.insert_entry(key.0.as_str(), &value.0);
                count += 1;
                bar.inc();
            });
            bar.finish();
            raw.flush_entry_cache();

            let mut count = 0;
            dict.traverse_token(&mut |key: &EntryKey, value: &EntryValue| {
                raw.insert_token(key.0.as_str(), &value.0);
                count += 1;
                bar.inc();
            });
            bar.finish();
            raw.flush_token_cache();
        }
        (EXT_RAW_ENTRY, EXT_ENTRY) | (EXT_RAW_RESOURCE, EXT_RESOURCE) => {
            let dict = RawDict::from(source);
            dict.to_beluga(&target).await;
        }
        _ => panic!("Invalid transform format"),
    }
}
