pub fn variance_buckets(values: &[u8], labels: &[f32]) -> Vec<(usize, f32)> {
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

pub fn variance_buckets_to_variances<I>(buckets: I) -> impl Iterator<Item = f32>
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
