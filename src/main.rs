extern crate csv;

use std::collections::HashMap;
use std::io;
use std::mem;

#[derive(Debug)]
struct Dataset {
    inputs: HashMap<String, Column>,
    labels: Vec<f32>,
}

impl Dataset {
    fn from_csv<F, S>(mut rdr: csv::Reader<F>, label_name: S) -> io::Result<Self>
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
                return Err(io::ErrorKind::NotFound.into());
            };

        for result in records {
            let record = result?;
            for (index, _, ref mut builder) in &mut builders {
                builder.append(&record[*index]);
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

    fn new() -> Self {
        Dataset {
            inputs: Default::default(),
            labels: Default::default(),
        }
    }

    fn partition<F>(&self, mut predicate: F) -> (Self, Self)
    where
        F: FnMut(usize) -> bool,
    {
        let mut left = Self::new();
        let mut right = Self::new();
        let (left_labels, right_labels) = self.labels.partition_by_index(&mut predicate);
        left.labels = left_labels;
        right.labels = right_labels;

        for (colname, column) in &self.inputs {
            let (left_col, right_col) = column.partition(&mut predicate);
            left.inputs.insert(colname.clone(), left_col);
            right.inputs.insert(colname.clone(), right_col);
        }

        (left, right)
    }
}

#[derive(Debug)]
enum Column {
    Float(Vec<f32>),

    QuantizedFloat(Vec<f32>, Vec<u8>),

    // FIXME: this is super inefficient.
    String(Vec<String>),
}

trait VecExt
where
    Self: Sized,
{
    fn partition_by_index<F>(&self, predicate: F) -> (Self, Self)
    where
        F: FnMut(usize) -> bool;
}

impl<T: Clone> VecExt for Vec<T> {
    fn partition_by_index<F>(&self, mut predicate: F) -> (Self, Self)
    where
        F: FnMut(usize) -> bool,
    {
        let mut left = vec![];
        let mut right = vec![];
        for (i, item) in self.iter().enumerate() {
            if predicate(i) {
                left.push(item.clone());
            } else {
                right.push(item.clone());
            }
        }
        (left, right)
    }
}

impl Column {
    fn partition<F>(&self, predicate: F) -> (Self, Self)
    where
        F: FnMut(usize) -> bool,
    {
        use Column::*;
        match self {
            Float(data) => {
                let (left, right) = data.partition_by_index(predicate);
                (Float(left), Float(right))
            }
            QuantizedFloat(quantiles, data) => {
                let (left, right) = data.partition_by_index(predicate);
                (
                    QuantizedFloat(quantiles.clone(), left),
                    QuantizedFloat(quantiles.clone(), right),
                )
            }
            Column::String(data) => {
                let (left, right) = data.partition_by_index(predicate);
                (Column::String(left), Column::String(right))
            }
        }
    }
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
    let mut sums: Vec<f32> = (0..256).map(|_| 0.).collect();
    let mut buckets: Vec<(usize, f32)> = (0..256).map(|_| (0, 0.)).collect();
    for (value, label) in values.iter().copied().zip(labels) {
        buckets[value as usize].0 += 1;
        sums[value as usize] += label;
    }
    let avgs: Vec<f32> = {
        let mut count_so_far = 0usize;
        let mut sum_so_far = 0f32;
        sums.iter()
            .copied()
            .zip(&buckets)
            .map(|(sum, (count, _))| {
                count_so_far += count;
                sum_so_far += sum;
                sum_so_far / count_so_far as f32
            })
            .collect()
    };

    for (value, label) in values.iter().copied().zip(labels) {
        let avg = avgs[value as usize];
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

fn split(dataset: &Dataset) -> Option<(String, f32, Dataset, Dataset)> {
    let mut candidates = dataset
        .inputs
        .iter()
        .flat_map(|(colname, column)| match column {
            Column::QuantizedFloat(quantiles, data) => {
                let buckets = variance_buckets(&data, &dataset.labels);
                let variances: Vec<_> =
                    variance_buckets_to_variances(buckets.iter().copied()).collect();
                /*
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
                */
                variances
                    .iter()
                    .copied()
                    .enumerate()
                    .filter(|(_, v)| !v.is_nan())
                    .min_by(|(_, a), (_, b)| compare_f32(*a, *b))
                    .and_then(|(i, v)| {
                        if i + 1 < quantiles.len() {
                            Some((colname.clone(), i + 1, quantiles[i + 1], v))
                        } else {
                            None
                        }
                    })
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return None;
    }

    candidates.sort_by(|(_, _, _, v1), (_, _, _, v2)| compare_f32(*v1, *v2));
    println!("*****************************");
    println!("{:#?}", candidates);

    let (colname, threshold_index, threshold, _variance) = candidates.swap_remove(0);
    let (left, right) = match &dataset.inputs[&colname] {
        Column::QuantizedFloat(_, data) => {
            dataset.partition(|i| (data[i] as usize) < threshold_index)
        }
        _ => panic!(
            "Splitting on a non-QuantizedFloat column {:?} not supported",
            colname
        ),
    };
    Some((colname, threshold, left, right))
}

#[derive(Debug)]
enum Tree<T> {
    Leaf(T),
    Branch(String, f32, Box<Tree<T>>, Box<Tree<T>>),
}

impl<T> Tree<T> {
    fn map<F, B>(self, f: &F) -> Tree<B>
    where
        F: Fn(T) -> B,
    {
        use Tree::*;
        match self {
            Leaf(x) => Leaf(f(x)),
            Branch(colname, threshold, left, right) => Branch(
                colname,
                threshold,
                Box::new(left.map(f)),
                Box::new(right.map(f)),
            ),
        }
    }
}

fn build_tree(dataset: Dataset, max_depth: usize) -> Tree<Dataset> {
    match split(&dataset) {
        Some((colname, threshold, left, right)) if max_depth > 0 => {
            println!("{} < {}", colname, threshold);
            Tree::Branch(
                colname,
                threshold,
                Box::new(build_tree(left, max_depth - 1)),
                Box::new(build_tree(right, max_depth - 1)),
            )
        }
        _ => Tree::Leaf(dataset),
    }
}

fn main() -> io::Result<()> {
    let mut dataset = Dataset::from_csv(csv::Reader::from_reader(io::stdin()), "Humidity")?;
    for (_, column) in dataset.inputs.iter_mut() {
        quantize_column(column);
    }
    let tree = build_tree(dataset, 5);
    println!("{:#?}", tree.map(&|d| d.labels.len()));
    Ok(())
}
