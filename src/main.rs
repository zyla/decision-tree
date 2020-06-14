extern crate csv;

use std::io;

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

fn main() -> io::Result<()> {
    set_timing(true);

    let mut dataset = timed("load csv", || {
        Dataset::from_csv(csv::Reader::from_reader(io::stdin()), "Humidity")
    })?;
    timed("quantize", || {
        for (_, column) in dataset.inputs.iter_mut() {
            quantize_column_by_random_sample(column);
        }
    });
    let tree = timed("build tree", || build_tree(dataset, 5));
    println!("{:#?}", tree.map(&|d| mean(&d.labels)));
    Ok(())
}
