extern crate regex;
extern crate rocksdb;

#[macro_use]
extern crate lazy_static;

use regex::Regex;
use rocksdb::{Options, SliceTransform, DB};
use std::collections::HashSet;
use std::env;

lazy_static! {
    static ref RE: Regex =
        Regex::new(r"^.*?-(?P<hash>0x[a-fA-F0-9]+)$",).expect("Regex RE is correct");
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Target {
    Balance(String),
    Storage(String, String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Access {
    target: Target,
    mode: AccessMode,
}

impl Access {
    fn from_string(raw: &str) -> Self {
        let mode = match &raw[0..1] {
            "R" => AccessMode::Read,
            "W" => AccessMode::Write,
            _ => panic!("TODO"),
        };

        let target = match &raw[1..2] {
            "B" => {
                let address = raw[3..45].to_owned();
                Target::Balance(address)
            }
            "S" => {
                let address = raw[3..45].to_owned();
                let entry = raw[47..113].to_owned();
                Target::Storage(address, entry)
            }
            _ => panic!("TODO2"),
        };

        Access { target, mode }
    }
}

#[derive(Debug)]
struct TransactionInfo {
    tx_hash: String,
    accesses: HashSet<Access>,
}

fn tx_hash(raw: &str) -> &str {
    RE.captures(raw)
        .expect(&format!("Expected to tx hash in {}", raw))
        .name("hash")
        .map_or("", |m| m.as_str())
}

fn accesses(raw: &str) -> HashSet<Access> {
    raw.trim_matches(|ch| ch == '{' || ch == '}')
        .split(", ")
        .map(Access::from_string)
        .collect()
}

fn tx_infos(db: &DB, block: u64) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.map(|(key, value)| {
        let key = std::str::from_utf8(&*key).expect("key read is valid string");
        let value = std::str::from_utf8(&*value).expect("value read is valid string");

        TransactionInfo {
            tx_hash: tx_hash(key).to_owned(),
            accesses: accesses(value).to_owned(),
        }
    })
    .collect()
}

fn check_block_conflicts(block_number: u64, tx_infos: Vec<TransactionInfo>, detailed: bool) -> i32 {
    let num_txs = tx_infos.len();

    println!(
        "Checking conflicts in block #{} ({} txs)...",
        block_number, num_txs,
    );

    if num_txs == 0 {
        // println!("Empty block, no conflicts\n");
        println!("{};0;0;0", block_number);
        return 0;
    }

    if num_txs == 1 {
        // println!("Singleton block, no conflicts\n");
        println!("{};0;0;0", block_number);
        return 0;
    }

    let mut num_conflicting_pairs = 0;
    let mut num_conflicting_pairs_balance = 0;
    let mut num_conflicting_pairs_storage = 0;

    let mut num_conflicts_in_block = 0;
    let mut num_balance_conflicts_in_block = 0;
    let mut num_storage_conflicts_in_block = 0;

    for ii in 0..(num_txs - 1) {
        for jj in (ii + 1)..num_txs {
            let tx_a = &tx_infos[ii];
            let tx_b = &tx_infos[jj];

            let mut num_balance_conflicts = 0;
            let mut num_balance_rw_conflicts = 0;
            let mut num_balance_ww_conflicts = 0;

            let mut num_storage_conflicts = 0;
            let mut num_storage_rw_conflicts = 0;
            let mut num_storage_ww_conflicts = 0;

            let mut balance_conflict = false;
            let mut storage_conflict = false;

            for access in &tx_a.accesses {
                match access {
                    Access {
                        target: Target::Balance(addr),
                        mode: AccessMode::Read,
                    } => {
                        if tx_b.accesses.contains(&Access {
                            target: Target::Balance(addr.clone()),
                            mode: AccessMode::Write,
                        }) {
                            num_conflicts_in_block += 1;
                            num_balance_conflicts_in_block += 1;
                            num_balance_conflicts += 1;
                            num_balance_rw_conflicts += 1;
                            balance_conflict = true;
                        }
                    }
                    Access {
                        target: Target::Balance(addr),
                        mode: AccessMode::Write,
                    } => {
                        if tx_b.accesses.contains(&Access {
                            target: Target::Balance(addr.clone()),
                            mode: AccessMode::Read,
                        }) {
                            num_conflicts_in_block += 1;
                            num_balance_conflicts_in_block += 1;
                            num_balance_conflicts += 1;
                            num_balance_rw_conflicts += 1;
                            balance_conflict = true;
                        }

                        if tx_b.accesses.contains(&Access {
                            target: Target::Balance(addr.clone()),
                            mode: AccessMode::Write,
                        }) {
                            num_conflicts_in_block += 1;
                            num_balance_conflicts_in_block += 1;
                            num_balance_conflicts += 1;
                            num_balance_ww_conflicts += 1;
                            balance_conflict = true;
                        }
                    }
                    Access {
                        target: Target::Storage(addr, entry),
                        mode: AccessMode::Read,
                    } => {
                        if tx_b.accesses.contains(&Access {
                            target: Target::Storage(addr.clone(), entry.clone()),
                            mode: AccessMode::Write,
                        }) {
                            num_conflicts_in_block += 1;
                            num_storage_conflicts_in_block += 1;
                            num_storage_conflicts += 1;
                            num_storage_rw_conflicts += 1;
                            storage_conflict = true;
                        }
                    }
                    Access {
                        target: Target::Storage(addr, entry),
                        mode: AccessMode::Write,
                    } => {
                        if tx_b.accesses.contains(&Access {
                            target: Target::Storage(addr.clone(), entry.clone()),
                            mode: AccessMode::Read,
                        }) {
                            num_conflicts_in_block += 1;
                            num_storage_conflicts_in_block += 1;
                            num_storage_conflicts += 1;
                            num_storage_rw_conflicts += 1;
                            storage_conflict = true;
                        }

                        if tx_b.accesses.contains(&Access {
                            target: Target::Storage(addr.clone(), entry.clone()),
                            mode: AccessMode::Write,
                        }) {
                            num_conflicts_in_block += 1;
                            num_storage_conflicts_in_block += 1;
                            num_storage_conflicts += 1;
                            num_storage_ww_conflicts += 1;
                            storage_conflict = true;
                        }
                    }
                }
            }

            if balance_conflict {
                num_conflicting_pairs_balance += 1;
            }

            if storage_conflict {
                num_conflicting_pairs_storage += 1;
            }

            if balance_conflict || storage_conflict {
                num_conflicting_pairs += 1;

                if detailed {
                    println!(
                        "    {} - {}: b = {} ({}/{}), s = {} ({}/{})",
                        tx_a.tx_hash,
                        tx_b.tx_hash,
                        num_balance_conflicts,
                        num_balance_rw_conflicts,
                        num_balance_ww_conflicts,
                        num_storage_conflicts,
                        num_storage_rw_conflicts,
                        num_storage_ww_conflicts,
                    );
                }
            }
        }
    }

    if num_conflicts_in_block == 0 {
        // println!("No conflicts in block\n");
        println!("{};0;0;0", block_number);
        return 0;
    }

    // println!(
    //     "number of conflicting tx pairs in block #{}: {} ({}/{}) (#conflicts = {} ({}/{}))\n",
    //     block_number,
    //     num_conflicting_pairs,
    //     num_conflicting_pairs_balance,
    //     num_conflicting_pairs_storage,
    //     num_conflicts_in_block,
    //     num_balance_conflicts_in_block,
    //     num_storage_conflicts_in_block,
    // );

    println!(
        "{};{};{};{}",
        block_number,
        num_conflicting_pairs,
        num_conflicting_pairs_balance,
        num_conflicting_pairs_storage
    );

    num_conflicting_pairs
}

fn main() {
    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        println!("Usage: evm-trace-extract [db-path:str] [from-block:int] [to-block:int] [detailed:bool]");
        return;
    }

    let path = &args[1][..];
    let from = args[2]
        .parse::<u64>()
        .expect("from-block should be a number");
    let to = args[3].parse::<u64>().expect("to-block should be a number");
    let detailed = args[4].parse::<bool>().expect("to-block should be a bool");

    // open db
    let prefix_extractor = SliceTransform::create_fixed_prefix(8);

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_prefix_extractor(prefix_extractor);

    let db = DB::open(&opts, path).expect("can open db");

    // check range
    let latest_raw = db
        .get(b"latest")
        .expect("get latest should succeed")
        .expect("latest should exist");

    let latest = std::str::from_utf8(&latest_raw[..])
        .expect("parse to string succeed")
        .parse::<u64>()
        .expect("parse to int should succees");

    if to > latest {
        println!("Latest header in trace db: #{}", latest);
        return;
    }

    // process
    let mut max_conflicts = 0;
    let mut max_conflicts_block = 0;

    for block in from..=to {
        let tx_infos = tx_infos(&db, block);
        let num = check_block_conflicts(block, tx_infos, detailed);

        if num > max_conflicts {
            max_conflicts = num;
            max_conflicts_block = block;
        }
    }

    println!(
        "Block with most conflicts: #{} ({})",
        max_conflicts_block, max_conflicts
    );
}
