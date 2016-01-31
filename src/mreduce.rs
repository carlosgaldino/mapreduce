use std::fs::File;
use std::io::{ BufReader, BufWriter };
use std::io::prelude::*;
use std::hash::{ Hash, SipHasher, Hasher };
use std::collections::BTreeMap;

struct MapReduce {
    nmap: usize,
    nreduce: usize,
    filename: String,
}

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

fn map_filename(filename: &str, job_count: usize) -> String {
    format!("mrtmp.{}-{}", filename, job_count)
}

fn reduce_filename(filename: &str, map_job: usize, reduce_job: usize) -> String {
    format!("{}-{}", map_filename(filename, map_job), reduce_job)
}

fn merge_filename(filename: &str, job_count: usize) -> String {
    format!("mrtmp.{}-res-{}", filename, job_count)
}

fn split(filename: &str, mr: &MapReduce) {
    println!("split {}", filename);

    let f = File::open(filename).expect("split");

    let metadata = f.metadata().expect("split metadata");
    let size = metadata.len() as usize;
    let nchunk = size / mr.nmap;

    let mut outfile = File::create(map_filename(filename, 0)).expect("split create outfile");

    let mut reader = BufReader::new(f);
    let mut buf = String::new();
    let mut i = 0;
    let mut m = 1;

    loop {
        let len = reader.read_line(&mut buf).unwrap();

        if i > nchunk * m {
            outfile.write(buf.as_bytes());
            buf.clear();

            if m == mr.nmap {
                break;
            }

            outfile = File::create(map_filename(filename, m)).expect("split create outfile");
            m += 1;
        }

        i += len;
    }
}

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = SipHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn do_map<M>(job_number: usize, filename: &str, nreduce: usize, map: &M)
    where M: Fn(&str) -> Vec<KeyValue> {

    let name = map_filename(filename, job_number);
    let mut f = File::open(&name).expect("do_map");

    let metadata = f.metadata().expect("do_map metadata");
    let size = metadata.len() as usize;

    println!("do_map read split {} {}", name, size);

    let mut buf = String::new();

    f.read_to_string(&mut buf);

    let res = map(&buf);

    for r in 0..nreduce {
        let f = File::create(reduce_filename(filename, job_number, r)).expect("do_map create");
        let mut buf = BufWriter::new(f);
        for kv in &res {
            if hash(&kv.key) % (nreduce as u64) == r as u64 {
                write!(buf, "{} {}\n", kv.key, kv.value).expect("do_map marshall");
            }
        }
        buf.flush().expect("do_map flushing");
    }
}

fn do_reduce<R>(job_number: usize, filename: &str, nmap: usize, reduce: &R)
    where R: Fn(&str, &Vec<String>) -> String {

    let mut hash: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for i in 0..nmap {
        let name = reduce_filename(filename, i, job_number);
        println!("do_reduce read {}", name);

        let f = File::open(name).expect("do_reduce");

        let reader = BufReader::new(f);

        for line in reader.lines() {
            let line = line.unwrap();
            let kv = line.split_whitespace().collect::<Vec<_>>();
            let k = kv[0];
            let v = kv[1];

            if hash.contains_key(k) {
                hash.get_mut(k).unwrap().push(String::from(v));
            } else {
                hash.insert(String::from(k), vec![String::from(v)]);
            }
        }
    }

    let f = File::create(merge_filename(filename, job_number)).expect("do_reduce create merge");
    let mut buf = BufWriter::new(f);

    for (k, v) in hash.iter() {
        write!(buf, "{} {}\n", k, reduce(k, v)).expect("do_reduce marshall");
    }

    buf.flush().expect("do_reduce flushing");
}

fn merge(mr: &MapReduce) {
    println!("Merge phase");

    let mut hash: BTreeMap<String, String> = BTreeMap::new();

    for i in 0..mr.nreduce {
        let name = merge_filename(&mr.filename, i);
        println!("Merge read {}", name);

        let f = File::open(name).expect("merge");

        let reader = BufReader::new(f);

        for line in reader.lines() {
            let line = line.unwrap();
            let kv = line.split_whitespace().collect::<Vec<_>>();
            let k = String::from(kv[0]);
            let v = String::from(kv[1]);

            hash.insert(k, v);
        }
    }

    let f = File::create(format!("mrtmp.{}", mr.filename)).expect("merge final create");
    let mut buf = BufWriter::new(f);

    for (k, v) in hash.iter() {
        write!(buf, "{} {}\n", k, v).expect("merge marshall");
    }

    buf.flush().expect("merge flushing");
}

pub fn run_single<M, R>(nmap: usize, nreduce: usize, filename: String, map: M, reduce: R)
    where M: Fn(&str) -> Vec<KeyValue>,
          R: Fn(&str, &Vec<String>) -> String {

    let mr = MapReduce { nmap: nmap, nreduce: nreduce, filename: filename.clone() };

    split(&filename, &mr);

    for i in 0..nmap {
        do_map(i, &mr.filename, mr.nreduce, &map);
    }

    for i in 0..nreduce {
        do_reduce(i, &mr.filename, mr.nmap, &reduce);
    }

    merge(&mr);
}
