extern crate mapreduce;

use std::env;
use mapreduce::mreduce::{self, KeyValue};

fn main() {
    let mut args = env::args().skip(1);
    let last = env::args().last();
    if args.len() != 3 {
        println!("three args please.");
    } else if last.unwrap() == "sequential" {
        mreduce::run_single(5, 3, args.nth(1).unwrap(), map, reduce);
    } else {
        println!("not implemented yet.");
    }
}

fn map(content: &str) -> Vec<KeyValue> {
    let mut v = Vec::new();

    for w in content.split_whitespace() {
        let w: String = w.chars().filter(|c| c.is_alphanumeric()).collect();

        if !w.is_empty() {
            v.push(KeyValue {
                key: w,
                value: "1".to_string(),
            });
        }
    }

    v
}

fn reduce(key: &str, values: &Vec<String>) -> String {
    let count = values.iter().fold(0, |acc, v| v.parse::<usize>().unwrap() + acc);

    format!("{}", count)
}
