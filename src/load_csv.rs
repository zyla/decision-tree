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

        let mut chunk: Vec<_> = Vec::with_capacity(1000);
        for result in records {
            let record = result?;
            chunk.push(record);
            if chunk.len() == chunk.capacity() {
                for (index, _, ref mut builder) in &mut builders {
                    builder.append(
                        &chunk
                            .iter()
                            .map(|record| &record[*index])
                            .collect::<Vec<&str>>(),
                    );
                }
                chunk.clear();
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

fn parse_f32(s: &str) -> f32 {
    let mut value = 0f32;
    let mut in_fraction = false;
    let mut fraction_multiplier = 0.1f32;
    let mut chars = s.chars();
    let (sign, rest) = match chars.next() {
        Some('-') => (-1., chars),
        _ => (1., s.chars()),
    };
    for c in rest {
        match c.to_digit(10) {
            Some(val) => {
                if in_fraction {
                    value += (val as f32) * fraction_multiplier;
                    fraction_multiplier *= 0.1;
                } else {
                    value *= 10.;
                    value += val as f32;
                }
            }
            None => {
                in_fraction = true;
            }
        }
    }
    value * sign
}

trait ColumnBuilder {
    fn append(&mut self, values: &[&str]);
    fn build(&mut self) -> Column;
}

#[derive(Debug, Default)]
struct FloatColumnBuilder(Vec<f32>);

impl ColumnBuilder for FloatColumnBuilder {
    fn append(&mut self, values: &[&str]) {
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