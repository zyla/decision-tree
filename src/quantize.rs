use crate::dataset::*;
use crate::util::*;
use rand::Rng;

pub(crate) fn quantiles_from_random_sample(values: &[f32], nquantiles: usize) -> Vec<f32> {
    let samples_per_quantile = 100;
    let nsamples = samples_per_quantile * nquantiles;

    let mut rng = rand::thread_rng();
    let mut indices: Vec<usize> = (0..nsamples).map(|_| rng.gen_range(0, values.len())).collect();
    let mut samples: Vec<f32> = indices.iter().copied().map(|i| values[i]).collect();
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
        Column::Float(values) => {
            let quantiles = quantiles_from_random_sample(&values, 255);
            let qdata = quantize(&values, &quantiles);
//            println!("{:?}", values.iter().zip(&qdata).take(1000).collect::<Vec<_>>());
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
            let step = (max - min) / ((nquantiles + 1) as f32);
            let quantiles = (1..nquantiles + 1)
                .map(|i| min + step * (i as f32))
                .collect();
            let step_inv = ((nquantiles - 1) as f32) / (max - min);

            let qdata = values
                .iter()
                .map(|value| ((value - min) / step).floor() as u8)
                .collect();
            *col = Column::QuantizedFloat(quantiles, qdata)
        }
        _ => {}
    }
}
