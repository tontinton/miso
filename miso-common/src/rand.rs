/// Knuth's multiplicative hash. For tests/benchmarks only.
pub fn pseudo_random(seed: usize) -> usize {
    seed.wrapping_mul(2654435761) ^ 0x12345678
}
