use memchr::{memchr, memchr_iter};
use memmap::Mmap;
use std::io;

pub struct Reader<T> {
    file: T,
    num_columns: usize,
    pos: usize,
}

impl<T> Reader<T> {
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}

#[derive(Debug)]
pub struct Record {
    chunks: Vec<(usize, usize)>,
}

impl Record {
    fn new() -> Self {
        Record { chunks: vec![] }
    }
}

impl<T: AsRef<[u8]>> Reader<T> {
    pub fn new(file: T) -> Self {
        let data: &[u8] = file.as_ref();
        let newline_pos = memchr(b'\n', data).unwrap_or(0);
        let num_columns = memchr_iter(b',', &data[0..newline_pos]).count() + 1;
        Reader {
            file,
            num_columns,
            pos: newline_pos + 1,
        }
    }

    pub fn read_record(&mut self, record: &mut Record) -> bool {
        let data: &[u8] = self.file.as_ref();

        if self.pos >= data.len() {
            return false;
        }

        record.chunks.clear();

        for _ in 0..self.num_columns - 1 {
            match memchr(b',', &data[self.pos..]) {
                Some(len) => {
                    record.chunks.push((self.pos, len));
                    self.pos += len + 1;
                }
                None => {
                    return false;
                }
            }
        }
        let rest = &data[self.pos..];
        let last_len = memchr(b'\n', rest).unwrap_or(rest.len());
        record.chunks.push((self.pos, last_len));
        self.pos += last_len + 1;
        return true;
    }

    pub fn get_datum<'a>(&'a self, record: &Record, column: usize) -> &'a [u8] {
        let (pos, len) = record.chunks[column];
        &self.file.as_ref()[pos..pos + len]
    }
}

#[test]
fn test_reader() {
    let mut rdr = Reader::new(b"a,b,c\nfoo,bar,baz\nqux,beep,foo\n");
    let mut records = vec![];
    loop {
        let mut record = Record::new();
        if !rdr.read_record(&mut record) {
            break;
        }
        records.push(record);
    }

    let data: Vec<Vec<String>> = records
        .iter()
        .map(|record| {
            (0..rdr.num_columns())
                .map(|i| String::from_utf8(rdr.get_datum(record, i).to_vec()).unwrap())
                .collect()
        })
        .collect();
    assert_eq!(
        data,
        vec![vec!["foo", "bar", "baz",], vec!["qux", "beep", "foo",]]
    );
}
