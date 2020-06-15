use memchr::memchr;

pub struct Reader<'a> {
    data: &'a [u8],
    header: Record,
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn num_columns(&self) -> usize {
        self.header.len()
    }

    pub fn header(&self) -> &Record {
        &self.header
    }
}

#[derive(Debug)]
pub struct Record {
    chunks: Vec<(usize, usize)>,
}

impl Record {
    pub fn new() -> Self {
        Record { chunks: vec![] }
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let newline_pos = memchr(b'\n', data).unwrap_or(0);
        let mut header = Record::new();
        {
            let mut pos = 0;
            while let Some(len) = memchr(b',', &data[pos..newline_pos]) {
                header.chunks.push((pos, len));
                pos += len + 1;
            }
            header.chunks.push((pos, newline_pos - pos));
        }
        Reader {
            data,
            header,
            pos: newline_pos + 1,
        }
    }

    pub fn read_record(&mut self, record: &mut Record) -> bool {
        let data = self.data;

        if self.pos >= data.len() {
            return false;
        }

        record.chunks.clear();

        for _ in 0..self.num_columns() - 1 {
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

    pub fn get_datum(&self, record: &Record, column: usize) -> &[u8] {
        let (pos, len) = record.chunks[column];
        &self.data[pos..pos + len]
    }

    pub fn record_iter<'b>(&'b self, record: &'b Record) -> impl Iterator<Item = &'b [u8]> + 'b {
        record
            .chunks
            .iter()
            .map(move |(pos, len)| &self.data[*pos..*pos + *len])
    }
}

#[cfg(test)]
fn record_to_strings<'a>(rdr: &Reader<'a>, record: &Record) -> Vec<String> {
    (0..rdr.num_columns())
        .map(|i| String::from_utf8(rdr.get_datum(record, i).to_vec()).unwrap())
        .collect()
}

#[test]
fn test_reader() {
    let input: &[u8] = b"Col1,b,col3\nfoo,bar,baz\nqux,beep,foo\n";
    let mut rdr = Reader::new(input);

    assert_eq!(
        record_to_strings(&rdr, &rdr.header()),
        vec!["Col1", "b", "col3"]
    );

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
        .map(|record| record_to_strings(&rdr, &record))
        .collect();
    assert_eq!(
        data,
        vec![vec!["foo", "bar", "baz",], vec!["qux", "beep", "foo",]]
    );
}
