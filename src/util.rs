pub fn mean(xs: &[f32]) -> f32 {
    xs.iter().copied().sum::<f32>() / (xs.len() as f32)
}

pub fn compare_f32(a: f32, b: f32) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

pub trait VecExt
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
