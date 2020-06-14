pub fn mean(xs: &[f32]) -> f32 {
    xs.iter().copied().sum::<f32>() / (xs.len() as f32)
}

pub fn compare_f32(a: f32, b: f32) -> std::cmp::Ordering {
    if a < b {
        std::cmp::Ordering::Less
    } else {
        std::cmp::Ordering::Greater
    }
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

#[inline(never)]
pub fn binary_search(s: &[f32], t: f32) -> usize {
    let mut size = s.len();
    if size == 0 {
        return 0;
    }
    let mut base = 0usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        base = if unsafe { *s.get_unchecked(mid) } > t {
            base
        } else {
            mid
        };
        size -= half;
    }
    // base is always in [0, size) because base <= mid.
    base
}

#[inline(never)]
#[allow(dead_code)]
pub fn linear_search(s: &[f32], t: f32) -> usize {
    for (i, &x) in s.iter().enumerate() {
        if t < x {
            return i;
        }
    }
    s.len()
}
