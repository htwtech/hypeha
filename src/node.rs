//! The node's own height, read off the stream files it writes every block.
//!
//! `/health` answers for `order_book_server` -- the height its *book* has been
//! applied to. The node underneath is another thing, and the two part company
//! exactly when it matters: after `order_book_server` restarts it replays from
//! a persisted state up to 10 000 blocks old, and for minutes its book is behind
//! a node that is perfectly current.
//!
//! The node's `visor_abci_state.json` looked like the place to read it, and is
//! not: measured on 2026-10-09 it moves in steps of ~70 blocks every ~4 s, and
//! was found 55 to 380 blocks behind the node's own output. What the node does
//! write every block is its stream files -- the very input `order_book_server`
//! tails -- one newline-delimited JSON line per block in each of
//! `node_fills_streaming`, `node_order_statuses_streaming` and
//! `node_raw_book_diffs_streaming`, every line opening with its
//! `"block_number"`. All three were found at the same block; fills is read here
//! because it is by far the smallest (~200 MB an hour against 10 and 31 GB) and
//! its lines are short.
//!
//! So the node's height is the `block_number` of the last complete line of the
//! newest file under `<data dir>/node_fills_streaming/hourly/<YYYYMMDD>/<HH>`.
//! Reading it needs wsarb on the node's machine, which it is, and keeps working
//! while `order_book_server` is down.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const STREAM: &str = "node_fills_streaming";
/// Read backwards from the end of the file this much at a time.
const CHUNK: u64 = 64 * 1024;
/// A last line longer than this is not something this file holds; give up
/// rather than read without bound.
const MAX_SCAN: u64 = 32 * 1024 * 1024;

/// The node's height under `data_dir`, or `None` if nothing readable is there.
/// The file work runs off the async runtime: it is small, but it is blocking IO.
pub async fn read_node_height(data_dir: String) -> Option<u64> {
    tokio::task::spawn_blocking(move || newest_block(Path::new(&data_dir))).await.ok().flatten()
}

pub fn newest_block(data_dir: &Path) -> Option<u64> {
    // The two newest files rather than one: on the hour the node opens the next
    // file before it has finished a line in it, and for that moment the height
    // is still the last line of the previous hour.
    newest_files(&data_dir.join(STREAM).join("hourly"), 2)
        .iter()
        .find_map(|p| last_block_number(p))
}

/// Newest first: day directories (`YYYYMMDD`, so name order is date order),
/// files within a day by modification time. The same order `order_book_server`
/// finds its newest file in.
fn newest_files(hourly: &Path, n: usize) -> Vec<PathBuf> {
    let mut days: Vec<PathBuf> = std::fs::read_dir(hourly)
        .map(|it| it.flatten().map(|e| e.path()).filter(|p| p.is_dir()).collect())
        .unwrap_or_default();
    days.sort();
    let mut out = Vec::new();
    for day in days.iter().rev().take(2) {
        let mut files: Vec<(SystemTime, PathBuf)> = std::fs::read_dir(day)
            .map(|it| {
                it.flatten()
                    .map(|e| e.path())
                    .filter(|p| p.is_file())
                    .filter_map(|p| Some((p.metadata().ok()?.modified().ok()?, p)))
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        out.extend(files.into_iter().rev().map(|(_, p)| p));
        if out.len() >= n {
            break;
        }
    }
    out.truncate(n);
    out
}

/// `block_number` of the last complete line in `path`. A partial tail -- the
/// node in the middle of writing a line -- is skipped, not misread.
fn last_block_number(path: &Path) -> Option<u64> {
    let mut f = File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    // `buf` holds the file from `start` to its end, growing backwards a chunk
    // at a time until it contains the last complete line from its beginning.
    let mut start = len;
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if let Some(end) = buf.iter().rposition(|&b| b == b'\n') {
            if let Some(prev) = buf[..end].iter().rposition(|&b| b == b'\n') {
                return extract_block_number(&String::from_utf8_lossy(&buf[prev + 1..end]));
            }
            if start == 0 {
                // The file's first line is its last complete one.
                return extract_block_number(&String::from_utf8_lossy(&buf[..end]));
            }
        }
        if start == 0 || len - start >= MAX_SCAN {
            return None;
        }
        let step = CHUNK.min(start);
        start -= step;
        f.seek(SeekFrom::Start(start)).ok()?;
        let mut chunk = vec![0_u8; step as usize];
        f.read_exact(&mut chunk).ok()?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
    }
}

/// The value of the first `"block_number":` in a line -- its header, where the
/// node puts it. The same reading `order_book_server` does.
fn extract_block_number(line: &str) -> Option<u64> {
    let idx = line.find("\"block_number\":")?;
    let rest = line[idx + "\"block_number\":".len()..].trim_start();
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("wsarb-node-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn line(block: u64) -> String {
        format!("{{\"local_time\":\"2026-10-09T19:54:00\",\"block_time\":\"2026-10-09T19:54:00\",\"block_number\":{block},\"events\":[]}}\n")
    }

    #[test]
    fn the_last_complete_line_is_read_and_a_partial_tail_is_not() {
        let dir = temp_dir("partial");
        let p = dir.join("19");
        let mut f = File::create(&p).unwrap();
        write!(f, "{}{}{{\"local_time\":\"x\",\"block_num", line(10), line(11)).unwrap();
        drop(f);
        assert_eq!(last_block_number(&p), Some(11));
    }

    #[test]
    fn a_last_line_longer_than_one_read_is_read_from_its_start() {
        let dir = temp_dir("long");
        let p = dir.join("19");
        let padding = "x".repeat((CHUNK * 3) as usize);
        let mut f = File::create(&p).unwrap();
        write!(f, "{}{{\"block_number\":12,\"events\":[\"{padding}\"]}}\n", line(11)).unwrap();
        drop(f);
        assert_eq!(last_block_number(&p), Some(12));
    }

    #[test]
    fn a_file_with_no_complete_line_has_no_height() {
        let dir = temp_dir("empty");
        let empty = dir.join("a");
        File::create(&empty).unwrap();
        assert_eq!(last_block_number(&empty), None);
        let partial = dir.join("b");
        write!(File::create(&partial).unwrap(), "{{\"block_number\":5").unwrap();
        assert_eq!(last_block_number(&partial), None);
        // A single complete line is the whole file.
        let one = dir.join("c");
        write!(File::create(&one).unwrap(), "{}", line(7)).unwrap();
        assert_eq!(last_block_number(&one), Some(7));
    }

    #[test]
    fn the_newest_hour_is_read_and_a_fresh_empty_one_falls_back() {
        let data = temp_dir("hourly");
        let day = data.join(STREAM).join("hourly").join("20261009");
        std::fs::create_dir_all(&day).unwrap();
        write!(File::create(day.join("18")).unwrap(), "{}{}", line(99), line(100)).unwrap();
        // On the hour: the next file exists before it holds a whole line.
        File::create(day.join("19")).unwrap();
        assert_eq!(newest_block(&data), Some(100));
        // Its first line lands.
        write!(std::fs::OpenOptions::new().append(true).open(day.join("19")).unwrap(), "{}", line(101)).unwrap();
        assert_eq!(newest_block(&data), Some(101));
    }

    #[test]
    fn nothing_under_the_data_dir_is_no_height() {
        let data = temp_dir("missing");
        assert_eq!(newest_block(&data), None);
    }
}
