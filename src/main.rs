extern crate csv;

use std::io;
use std::mem;

mod dataset;
mod load_csv;
mod quantize;
mod time;
mod tree;
mod util;
mod variance;

use crate::dataset::*;
use crate::quantize::*;
use crate::time::*;
use crate::tree::*;
use crate::util::*;
use crate::variance::*;

fn main() -> io::Result<()> {
    let mut dataset = Dataset::from_csv(csv::Reader::from_reader(io::stdin()), "Humidity")?;
    for (_, column) in dataset.inputs.iter_mut() {
        quantize_column(column, quantiles_from_random_sample);
    }
    let tree = build_tree(dataset, 5);
    println!("{:#?}", tree.map(&|d| mean(&d.labels)));
    Ok(())
}
