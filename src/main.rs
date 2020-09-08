extern crate regex;
extern crate rocksdb;
#[macro_use]
extern crate lazy_static;

mod stats;
mod transaction_info;

use rocksdb::{Options, SliceTransform, DB};
use stats::{BlockStats, TxPairStats};
use std::env;
use transaction_info::TransactionInfo;

fn tx_infos_from_db(db: &DB, block: u64) -> Vec<TransactionInfo> {
    use transaction_info::{parse_accesses, parse_tx_hash};

    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.map(|(key, value)| {
        let key = std::str::from_utf8(&*key).expect("key read is valid string");
        let value = std::str::from_utf8(&*value).expect("value read is valid string");

        TransactionInfo {
            tx_hash: parse_tx_hash(key).to_owned(),
            accesses: parse_accesses(value).to_owned(),
        }
    })
    .collect()
}

fn into_pairwise_iter<'a>(
    txs: &'a Vec<TransactionInfo>,
) -> impl Iterator<Item = (&'a TransactionInfo, &'a TransactionInfo)> {
    (0..(txs.len() - 1))
        .flat_map(move |ii| ((ii + 1)..txs.len()).map(move |jj| (ii, jj)))
        .map(move |(ii, jj)| (&txs[ii], &txs[jj]))
}

fn extract_tx_stats<'a>(pair: (&'a TransactionInfo, &'a TransactionInfo)) -> TxPairStats<'a> {
    use transaction_info::{Access, AccessMode, Target};

    let (tx_a, tx_b) = pair;
    let mut stats = TxPairStats::new(&tx_a.tx_hash, &tx_b.tx_hash);

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
                    stats.balance_rw += 1;
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
                    stats.balance_rw += 1;
                }

                if tx_b.accesses.contains(&Access {
                    target: Target::Balance(addr.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.balance_ww += 1;
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
                    stats.storage_rw += 1;
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
                    stats.storage_rw += 1;
                }

                if tx_b.accesses.contains(&Access {
                    target: Target::Storage(addr.clone(), entry.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.storage_ww += 1;
                }
            }
        }
    }

    stats
}

fn print_block_stats(block: u64, tx_infos: Vec<TransactionInfo>, detailed: bool) -> i32 {
    println!(
        "Checking conflicts in block #{} ({} txs)...",
        block,
        tx_infos.len(),
    );

    if tx_infos.len() == 0 {
        println!("Empty block, no conflicts\n");
        return 0;
    }

    if tx_infos.len() == 1 {
        println!("Singleton block, no conflicts\n");
        return 0;
    }

    let mut block_stats = BlockStats::new(block);

    for stats in into_pairwise_iter(&tx_infos).map(extract_tx_stats) {
        block_stats.accumulate(&stats);

        if detailed && stats.has_conflict() {
            println!("    {:?}", stats);
        }
    }

    if !block_stats.has_conflicts() {
        println!("No conflicts in block\n");
        return 0;
    }

    println!("{:?}\n", block_stats);

    block_stats.num_conflicting_pairs()
}

fn handle_blocks(db: &DB, blocks: impl Iterator<Item = u64>, detailed: bool) {
    let mut max_conflicts = 0;
    let mut max_conflicts_block = 0;

    for block in blocks {
        let tx_infos = tx_infos_from_db(&db, block);
        let num = print_block_stats(block, tx_infos, detailed);

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

fn print_block_stats_csv(block: u64, tx_infos: Vec<TransactionInfo>) {
    if tx_infos.len() == 0 || tx_infos.len() == 1 {
        println!("{},0,0,0", block);
        return;
    }

    let mut block_stats = BlockStats::new(block);

    for stats in into_pairwise_iter(&tx_infos).map(extract_tx_stats) {
        block_stats.accumulate(&stats);
    }

    println!(
        "{},{},{},{}",
        block,
        block_stats.num_conflicting_pairs(),
        block_stats.conflicting_pairs_balance,
        block_stats.conflicting_pairs_storage
    );
}

fn handle_blocks_csv(db: &DB, blocks: impl Iterator<Item = u64>) {
    // print header
    println!("block,conflicts,balance,storage");

    // print for each block
    for block in blocks {
        let tx_infos = tx_infos_from_db(&db, block);
        print_block_stats_csv(block, tx_infos);
    }
}

fn main() {
    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        println!("Usage: evm-trace-extract [db-path:str] [from-block:int] [to-block:int] [mode:(normal|detailed|csv)]");
        return;
    }

    let path = &args[1][..];

    let from = args[2]
        .parse::<u64>()
        .expect("from-block should be a number");

    let to = args[3].parse::<u64>().expect("to-block should be a number");
    let mode = &args[4][..];

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
    match mode {
        "csv" => handle_blocks_csv(&db, from..=to),
        "normal" => handle_blocks(&db, from..=to, false),
        "detailed" => handle_blocks(&db, from..=to, true),
        _ => {
            println!("mode should be one of: normal, detailed, csv");
            return;
        }
    }
}
