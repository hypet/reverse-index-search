use std::{collections::{BTreeMap}, env, fs::File, io::{BufReader, BufWriter, Read, Seek, SeekFrom}, os::windows::prelude::FileExt, sync::{Arc, Mutex}, thread, time::{Instant}};
use bincode::serialize_into;
use warp::{Filter};

struct Db {
    map: Arc<Mutex<BTreeMap<String, Box<Vec<u64>>>>>,
    db_file: Arc<File>
}

impl Db {
    // Multi-threaded file reading
    fn with_thread_number(src_file_path: String, out_file_path: String, threads: u16) -> Db {
        let inner_file = File::open(src_file_path.clone()).unwrap();
        let file_size = inner_file.metadata().unwrap().len();
        println!("File size: {}", &file_size);

        let db: Arc<Mutex<BTreeMap<String, Box<Vec<u64>>>>> = Arc::new(Mutex::from(BTreeMap::new()));
        let mut handlers = vec![];
        let part_size = file_size / (threads as u64);
        for thread_num in 0..threads {
            let db_copy = db.clone();
            let path = src_file_path.clone();
            let handler = thread::spawn(move || {
                process_part(path, (thread_num as u64) * part_size, (thread_num as u64 + 1) * part_size, db_copy);
            });
            handlers.push(handler);
        }
        for i in handlers {
            println!("Joining {:?}", &i.thread().id());
            i.join();
        }

        println!("DB size: {}", &((*db).lock().unwrap()).to_owned().len());
        let mut ser_file = BufWriter::new(File::create(out_file_path).unwrap());
        serialize_into(&mut ser_file, &((*db).lock().unwrap()).to_owned()).unwrap();

        let db_file = Arc::new(File::open(src_file_path.clone()).unwrap());
        Db { map: db, db_file: db_file }
    }

    // Single-threaded file reading
    fn new(src_file_path: String, out_file_path: String) -> Db {
        let start_time = Instant::now();
        let file = File::open(src_file_path.clone()).unwrap();
        let db_file = Arc::new(File::open(src_file_path).unwrap());
        let reader = BufReader::with_capacity(64 * 1024 * 1024, file);
        let mut db: BTreeMap<String, Box<Vec<u64>>> = BTreeMap::new();
        let mut pos = 0;
        let mut word = String::new();
        let mut word_start = 0;
        let mut is_word = false;
        for item in reader.bytes().into_iter() {
            let byte = item.unwrap();
            if byte == b' ' || byte == b'\n' || byte == b'\t' || byte == b'\'' || byte == b'[' || byte == b']' || byte == b'|' || byte == b'\\'  {
                if is_word {
                    db.entry(word).or_insert_with(|| Box::new(vec!(word_start)));
                    is_word = false;
                    word = String::new();
                }
            } else {
                if !is_word {
                    is_word = true;
                    word_start = pos;
                }
                word.push((byte as char).to_ascii_lowercase())
            }

            pos += 1;
            if pos % 10000000 == 0 {
                println!("Pos: {}", pos);
            }
        }

        println!("Finished reading in {:?}", Instant::now().checked_duration_since(start_time).unwrap());
        println!("DB size: {}", db.len());

        let mut ser_file = BufWriter::new(File::create(out_file_path).unwrap());
        serialize_into(&mut ser_file, &db).unwrap();

        Db { map: Arc::new(Mutex::from(db)), db_file }
    }
}

fn process_part(path: String, from: u64, to: u64, db: Arc<Mutex<BTreeMap<String, Box<Vec<u64>>>>>) {
    let tid = thread::current().id();
    let start_time = Instant::now();
    println!("{:?} process_part, from: {}, to: {}", tid, &from, &to);
    let inner_file = File::open(path.clone()).unwrap();
    let mut reader = BufReader::with_capacity(64 * 1024 * 1024, inner_file);
    match reader.seek(SeekFrom::Start(from)) {
        Err(e) => {
            println!("{:?} Error occurred on seek to {}: {}", tid, from, e);
            return;
        }
        Ok(_) => (),
    }
    let mut pos = from;
    let mut word = String::new();
    let mut word_start: u64 = 0;
    let mut is_word = false;
    for item in reader.bytes().into_iter() {
        let byte = item.unwrap();
        if byte == b' ' || byte == b'\n' || byte == b'\t' || byte == b'\'' || byte == b'[' || byte == b']' || byte == b'|' || byte == b'\\'  {
            if is_word {
                (*db).lock().as_deref_mut().unwrap().entry(word).or_insert(Box::new(vec!())).push(word_start.clone());
                is_word = false;
                word = String::new();
            }
        } else {
            if !is_word {
                is_word = true;
                word_start = pos.clone();
            }
            word.push((byte as char).to_ascii_lowercase())
        }

        pos += 1;
        if pos % 10000000 == 0 {
            println!("{:?} Pos: {}", tid, pos);
        }
        if pos == to {
            break;
        }
    }
    println!("{:?} Finished reading in {:?}", tid, Instant::now().checked_duration_since(start_time).unwrap());
}

const MAX_SEARCH_RESULTS: usize = 10;

#[tokio::main]
async fn main() {
    println!("{}", std::usize::MAX);
    let src_file_path = env::args().nth(1).expect("No source file path specified");
    let out_file_path = env::args().nth(2).expect("No output file path specified");
    let thread_num = env::args().nth(3).expect("No thread count specified");
    let db = Db::with_thread_number(src_file_path, out_file_path, thread_num.parse().unwrap());
    let db_arc = Arc::new(db);
    let port = 3030;

    let search = warp::get()
        .and(warp::path("search"))
        .and(warp::path::param::<String>())
        .map(move |text| { 
            warp::reply::html(search_text(text, db_arc.clone()))
        });

    println!("Web server has started on {}", port);
    warp::serve(search).run(([127, 0, 0, 1], 3030)).await
}

fn search_text(text: String, db: Arc<Db>) -> String {
    print!("Search phrase: {}", text);
    match &db.map.lock().unwrap().get(&text) {
        Some(v) => {
            let mut resp = String::new();
            println!(", vec size: {}", (*v).len());
            for (c, pos) in (*v).iter().enumerate() {
                println!("c: {}, pos: {}", &c, &pos);
                if c > MAX_SEARCH_RESULTS {
                    break;
                }
                let mut buffer = [0; 128];

                let left_offset = if *pos - 60 > 0 {
                    60 }
                else {
                    0};
                let seek_pos = *pos as u64 - left_offset;
                match db.db_file.seek_read(&mut buffer[..],seek_pos) {
                    Err(e) => {
                        println!("Error occurred on search seek to {}: {}", seek_pos, e);
                    }
                    Ok(_) => {
                        buffer.map(|b| resp.push(b as char));
                        resp.push('\n');
                        resp.push_str("<br/>");
                    },
                }
            }
            println!("resp: {}", resp);
            resp.to_string()
        },
        None => {
            println!("Nothing found!");
            "Nothing".to_string()
        },
    }
}
