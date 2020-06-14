use crate::util::VecExt;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Dataset {
    pub inputs: HashMap<String, Column>,
    pub labels: Vec<f32>,
}

impl Dataset {
    pub fn new() -> Self {
        Dataset {
            inputs: Default::default(),
            labels: Default::default(),
        }
    }

    pub fn partition<F>(&self, mut predicate: F) -> (Self, Self)
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
pub enum Column {
    Float(Vec<f32>),

    QuantizedFloat(Vec<f32>, Vec<u8>),

    // FIXME: this is super inefficient.
    String(Vec<String>),
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
