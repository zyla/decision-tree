use crate::dataset::*;
use std::collections::HashMap;

impl Dataset {
    pub fn from_csv<F, S>(mut rdr: csv::Reader<F>, label_name: S) -> std::io::Result<Self>
    where
        F: std::io::Read,
        S: AsRef<str>,
    {
        let header = rdr.headers()?.clone();
        let mut records = rdr.records();
        let mut builders: Vec<(usize, String, Box<dyn ColumnBuilder>)> =
            if let Some(Ok(record)) = records.next() {
                header
                    .iter()
                    .zip(record.into_iter())
                    .enumerate()
                    .flat_map(|(index, (colname, value))| {
                        if value.parse::<f32>().is_ok() {
                            Some((
                                index,
                                colname.into(),
                                Box::new(FloatColumnBuilder::default()) as Box<dyn ColumnBuilder>,
                            ))
                        } else {
                            // Don't parse Strings for now
                            // Some((colname.into(), Box::new(StringColumnBuilder::default()) as Box<dyn ColumnBuilder>))
                            None
                        }
                    })
                    .collect()
            } else {
                return Err(std::io::ErrorKind::NotFound.into());
            };

        let mut chunk: Vec<csv::ByteRecord> = Vec::with_capacity(32);
        while chunk.len() < chunk.capacity() {
            chunk.push(csv::ByteRecord::new());
        }
        let mut index_in_chunk = 0;
        while rdr.read_byte_record(&mut chunk[index_in_chunk])? {
            index_in_chunk += 1;
            if index_in_chunk == chunk.len() {
                for (index, _, ref mut builder) in &mut builders {
                    builder.append(
                        &chunk
                            .iter()
                            .map(|record| &record[*index])
                            .collect::<Vec<&[u8]>>(),
                    );
                }
                index_in_chunk = 0;
            }
        }
        let mut columns = builders
            .iter_mut()
            .map(|(_, ref colname, ref mut builder)| (colname.clone(), builder.build()))
            .collect::<HashMap<_, _>>();
        let labels = match columns.remove(label_name.as_ref()) {
            Some(Column::Float(data)) => data,
            _ => panic!("expected Float"),
        };
        Ok(Dataset {
            inputs: columns,
            labels,
        })
    }
}

#[allow(clippy::while_let_on_iterator)]
fn parse_f32(s: &[u8]) -> f32 {
    let mut ival = 0u32;
    let mut fraction_multiplier = 0.1f32;
    let mut chars = s.iter();
    let (sign, mut chars) = match chars.next() {
        Some(b'-') => (-1., chars),
        Some(b'N') => return std::f32::NAN,
        _ => (1., s.iter()),
    };
    while let Some(c) = chars.next() {
        match c {
            b'.' => break,
            _ => {
                ival *= 10;
                ival += (c - b'0') as u32;
            }
        }
    }
    let mut value = ival as f32;
    while let Some(c) = chars.next() {
        value += ((c - b'0') as f32) * fraction_multiplier;
        fraction_multiplier *= 0.1;
    }
    value * sign
}

trait ColumnBuilder {
    fn append(&mut self, values: &[&[u8]]);
    fn build(&mut self) -> Column;
}

#[derive(Debug, Default)]
struct FloatColumnBuilder(Vec<f32>);

impl ColumnBuilder for FloatColumnBuilder {
    fn append(&mut self, values: &[&[u8]]) {
        self.0.extend(values.iter().copied().map(parse_f32));
    }
    fn build(&mut self) -> Column {
        Column::Float(std::mem::replace(&mut self.0, vec![]))
    }
}

/*
#[derive(Debug, Default)]
struct StringColumnBuilder(Vec<String>);

impl ColumnBuilder for StringColumnBuilder {
    fn append(&mut self, value: &str) {
        self.0.push(value.into());
    }
    fn build(&mut self) -> Column {
        Column::String(mem::replace(&mut self.0, vec![]))
    }
}
*/
