use crate::dataset::*;
use crate::util::*;
use crate::variance::*;

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
    //println!("*****************************");
    //println!("{:#?}", candidates);

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
pub enum Tree<T> {
    Leaf(T),
    Branch(String, f32, Box<Tree<T>>, Box<Tree<T>>),
}

impl<T> Tree<T> {
    pub fn map<F, B>(self, f: &F) -> Tree<B>
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

pub fn build_tree(dataset: Dataset, max_depth: usize) -> Tree<Dataset> {
    match split(&dataset) {
        Some((colname, threshold, left, right)) if dataset.labels.len() >= 10 && max_depth > 0 => {
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
