extern crate csv;

use std::mem;
use std::io;
use std::collections::HashMap;

#[derive(Debug)]
struct Dataset {
    columns: HashMap<String, Column>,
}

impl Dataset {
    fn from_csv<F>(mut rdr: csv::Reader<F>) -> io::Result<Self> where F: std::io::Read {
        let mut records = rdr.records();
        let mut builders: Vec<Box<dyn ColumnBuilder>> =
                if let Some(Ok(record)) = records.next() {
                    record.iter().map(|value|
                                      if let Ok(_) = value.parse::<f32>() {
 Box::new(FloatColumnBuilder::default()) as Box<dyn ColumnBuilder>
                                      } else {
                                          Box::new(StringColumnBuilder::default())  as Box<dyn ColumnBuilder>
                                      }).collect()

                } else {
                    return Err(io::ErrorKind::NotFound.into());
                };

        for result in records {
            let record = result?;
            for (i, datum) in record.iter().enumerate() {
                builders[i].append(datum);
            }
        }
        Ok(Dataset{
            columns: rdr.headers()?.iter().zip(builders.into_iter()).map(|(colname, mut builder)| (colname.into(), builder.build())).collect()
        })
    }
}

#[derive(Debug)]
enum Column {
    Float(Vec<f32>),

    // TODO: this is super inefficient.
    String(Vec<String>),
}

trait ColumnBuilder {
    fn append(&mut self, value: &str);
    fn build(&mut self) -> Column;
}

#[derive(Debug, Default)]
struct FloatColumnBuilder(Vec<f32>);

impl ColumnBuilder for FloatColumnBuilder {
    fn append(&mut self, value: &str) {
        self.0.push(value.parse().unwrap());
    }
    fn build(&mut self) -> Column {
        Column::Float(mem::replace(&mut self.0, vec![]))
    }
}

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

fn main() -> io::Result<()> {
    let dataset = Dataset::from_csv(csv::Reader::from_reader(io::stdin()))?;
    println!("{:?}", dataset);
    Ok(())
}
