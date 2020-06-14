use crate::dataset::*;
use crate::util::*;
use rand::seq::IteratorRandom;

pub(crate) fn quantiles_from_random_sample(values: &[f32], nquantiles: usize) -> Vec<f32> {
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

/*
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
*/

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
#[inline(never)]
pub fn quantize_column_by_random_sample(col: &mut Column) {
    match col {
        Column::Float(data) => {
            let quantiles = quantiles_from_random_sample(&data, 255);
            let qdata = quantize(&data, &quantiles);
            *col = Column::QuantizedFloat(quantiles, qdata)
        }
        _ => {}
    }
}

#[allow(clippy::single_match)]
#[inline(never)]
pub fn quantize_column_uniform(col: &mut Column) {
    match col {
        Column::Float(values) => {
            let nquantiles = 255;
            let min = values
                .iter()
                .copied()
                .min_by(|a, b| compare_f32(*a, *b))
                .unwrap();
            let max = values
                .iter()
                .copied()
                .max_by(|a, b| compare_f32(*a, *b))
                .unwrap();
            let step = (max - min) / ((nquantiles - 1) as f32);
            let quantiles = (0..nquantiles + 1)
                .map(|i| min + step * (i as f32))
                .collect();
            let step_inv = ((nquantiles - 1) as f32) / (max - min);

            let qdata = values
                .iter()
                .map(|value| ((value - min) * step_inv) as u8)
                .collect();
            *col = Column::QuantizedFloat(quantiles, qdata)
        }
        _ => {}
    }
}
