extern crate csv;

use std::collections::HashMap;
use std::io;
use std::mem;

#[derive(Debug)]
struct Dataset {
    columns: HashMap<String, Column>,
}

impl Dataset {
    fn from_csv<F>(mut rdr: csv::Reader<F>) -> io::Result<Self>
    where
        F: std::io::Read,
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
                return Err(io::ErrorKind::NotFound.into());
            };

        for result in records {
            let record = result?;
            for (index, _, ref mut builder) in &mut builders {
                builder.append(&record[*index]);
            }
        }
        Ok(Dataset {
            columns: builders
                .iter_mut()
                .map(|(_, ref colname, ref mut builder)| (colname.clone(), builder.build()))
                .collect(),
        })
    }
}

#[derive(Debug)]
enum Column {
    Float(Vec<f32>),

    QuantizedFloat(Vec<f32>, Vec<u8>),

    // FIXME: this is super inefficient.
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

////////////////////////////////////////////////////////////////////////////////

use rand::seq::IteratorRandom;

fn compare_f32(a: f32, b: f32) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

fn quantiles(values: &[f32], nquantiles: usize) -> Vec<f32> {
    let samples_per_quantile = 100;

    let mut rng = rand::thread_rng();
    let mut samples: Vec<f32> = values
        .iter()
        .copied()
        .choose_multiple(&mut rng, nquantiles * samples_per_quantile);
    samples.sort_by(|a, b| compare_f32(*a, *b));

    samples
        .chunks(samples.len() / nquantiles)
        .map(|chunk| chunk[0])
        .collect()
}

#[allow(dead_code)]
fn counts(values: &[f32], quantiles: &[f32]) -> Vec<usize> {
    let mut counts = quantiles.iter().map(|_| 0).collect::<Vec<usize>>();
    counts.push(0);
    for value in values {
        let i = quantiles
            .binary_search_by(|a| compare_f32(*a, *value))
            .unwrap_or_else(|x| x);
        counts[i] += 1;
    }
    counts
}

fn quantize(values: &[f32], quantiles: &[f32]) -> Vec<u8> {
    values
        .iter()
        .map(|value| {
            quantiles
                .binary_search_by(|a| compare_f32(*a, *value))
                .unwrap_or_else(|x| x) as u8
        })
        .collect()
}

#[allow(clippy::single_match)]
fn quantize_column(col: &mut Column) {
    match col {
        Column::Float(data) => {
            let quantiles = quantiles(&data, 255);
            //            println!("{:?}", counts(&data, &quantiles));
            let qdata = quantize(&data, &quantiles);
            //            println!("{:?}", qdata);
            *col = Column::QuantizedFloat(quantiles, qdata)
        }
        _ => {}
    }
}

fn variance_buckets(values: &[u8], labels: &[f32]) -> Vec<(usize, f32)> {
    let avg: f32 = labels.iter().copied().sum::<f32>() / (labels.len() as f32);
    let mut buckets: Vec<(usize, f32)> = (0..256).map(|_| (0, 0.)).collect();
    for (value, label) in values.iter().copied().zip(labels) {
        buckets[value as usize].0 += 1;
        buckets[value as usize].1 += (label - avg) * (label - avg);
    }
    buckets
}

fn variance_buckets_to_variances<I>(buckets: I) -> impl Iterator<Item = f32>
where
    I: Iterator<Item = (usize, f32)>,
{
    let mut count_so_far = 0usize;
    let mut sum_so_far = 0f32;
    buckets.map(move |(count, sum)| {
        count_so_far += count;
        sum_so_far += sum;
        sum_so_far / count_so_far as f32
    })
}

fn main() -> io::Result<()> {
    let mut dataset = Dataset::from_csv(csv::Reader::from_reader(io::stdin()))?;
    let labels = match dataset.columns.remove("Humidity") {
        Some(Column::Float(data)) => data,
        _ => panic!("expected Float"),
    };
    for (_, column) in dataset.columns.iter_mut() {
        quantize_column(column);
    }
    let mut candidates = dataset
        .columns
        .iter()
        .flat_map(|(colname, column)| match column {
            Column::QuantizedFloat(quantiles, data) => {
                let buckets = variance_buckets(&data, &labels);
                let variances: Vec<_> =
                    variance_buckets_to_variances(buckets.iter().copied()).collect();
                println!(
                    "{} {:?}",
                    colname,
                    quantiles
                        .iter()
                        .zip(variances.iter())
                        .zip(buckets.iter())
                        .map(|((q, v), (n, _))| (q, n, v))
                        .collect::<Vec<_>>()
                );
                variances
                    .iter()
                    .copied()
                    .enumerate()
                    .filter(|(_, v)| !v.is_nan())
                    .min_by(|(_, a), (_, b)| compare_f32(*a, *b))
                    .and_then(|(i, v)| {
                        if i + 1 < quantiles.len() {
                            Some((colname.clone(), quantiles[i + 1], v))
                        } else {
                            None
                        }
                    })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(_, _, v1), (_, _, v2)| compare_f32(*v1, *v2));
    println!("*****************************");
    println!("{:#?}", candidates);
    Ok(())
}
