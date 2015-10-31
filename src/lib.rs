use std::num::{Int};
use std::cmp::max;
use num;

// https://github.com/bgamari/succinct.rs/blob/96f8a507ebeef0e89f96fd7888e43c6cc5b26133/src/utils.rs
pub fn div_ceil<T: Int>(a: T, b: T) -> T {
	if a % b != Int::zero() {
		a / b + Int::one()
	} else {
		a / b
	}
}

// https://github.com/PotHix/pothix-codes/blob/c83c40cd747f5c4565ae68707bef03fb9e161c83/rust/exercises/fractran/src/main.rs#L18
fn floor_log2(mut n: u64) -> u64 {
	let mut log = 0;
	while n > 1 {
		n = n >> 1;
		log = log + 1;
	}
	log
}

fn round_up_to_nearest(n: u64, nearest: u64) {
	return div_ceil(n, nearest) * nearest;
}

/// For tiny files (< 2KB), return 16
/// For non-tiny files, return (2^floor(log2(n)))/64
fn get_concealment_size(n: u64) {
	let average_wasteage = 1/128; // ~= .78%
	// Use an average wasteage of 1/128 (~.78%) and max wasteage of 1/64
	let ret = num::integer::div_floor(num::pow(2, floor_log2(n)), 64);
	max(16, ret)
}

/// Conceal a file size by rounding the size up log2-proportionally,
/// to a size 0% to 1.5625% of the original size.
fn conceal_size(n: u64) {
	let ret = round_up_to_nearest(max(1, n), get_concealment_size(n));
	assert!(ret >= n);
	ret
}

#[test]
fn test_get_concealment_size() {
	assert_eq!(get_concealment_size(0), 16);
	assert_eq!(get_concealment_size(1), 16);
	assert_eq!(get_concealment_size(128), 16);
	assert_eq!(get_concealment_size(256), 16);
	assert_eq!(get_concealment_size(1024), 16);
	assert_eq!(get_concealment_size(1.5*1024), 16);
	assert_eq!(get_concealment_size(2*1024), 32);
	assert_eq!(get_concealment_size(128*1024), 2048);

	assert_eq!(get_concealment_size(1024), 1024/64);

	assert_eq!(get_concealment_size(1024*1024), 1024*1024/64);

	assert_eq!(get_concealment_size(1024*1024*1024 - 1), 1024*1024*1024/128);
	assert_eq!(get_concealment_size(1024*1024*1024), 1024*1024*1024/64);
	assert_eq!(get_concealment_size(1024*1024*1024 + 1), 1024*1024*1024/64);
	assert_eq!(get_concealment_size(1024*1024*1024 + 1024*1024), 1024*1024*1024/64);
}

#[test]
fn test_conceal_size() {
	assert_eq!(conceal_size(0), 16);
	assert_eq!(conceal_size(1), 16);
	assert_eq!(conceal_size(128), 128);
	assert_eq!(conceal_size(256), 256);
	assert_eq!(conceal_size(1024), 1024);
	assert_eq!(conceal_size(1025), 1024 + 16);
	assert_eq!(conceal_size(1.5*1024), 1.5*1024);
	assert_eq!(conceal_size(2*1024), 2*1024);
	assert_eq!(conceal_size(2*1024+1), 2*1024 + 32);

	assert_eq!(conceal_size(1024*1024*1024 - 1), 1024*1024*1024);
	assert_eq!(conceal_size(1024*1024*1024), 1024*1024*1024);
	assert_eq!(conceal_size(1024*1024*1024 + 1), 1024*1024*1024 + 1024*1024*1024/64);
	assert_eq!(conceal_size(1024*1024*1024 + 1024*1024), 1024*1024*1024 + 1024*1024*1024/64);
}
