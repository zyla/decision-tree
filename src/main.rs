extern crate csv;

use std::io;

mod csv_parser;
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

    let filename = &std::env::args().collect::<Vec<_>>()[1];
    let file = std::fs::File::open(filename)?;

    let mut dataset = timed("load csv", || {
        Dataset::from_csv(file, "Humidity")
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
