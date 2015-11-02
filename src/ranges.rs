use std::cmp::{min, max};

#[derive(Debug, Eq, PartialEq)]
pub struct Range {
	start: u64,
	end: u64
}

impl Range {
	pub fn new(start: u64, end: u64) -> Range {
		assert!(start < end, "start must be < end; got start={}, end={}", start, end);
		Range { start: start, end: end }
	}
}

pub fn intersect(range1: Range, range2: Range) -> Option<Range> {
	// Range is the max of the beginnings to the min of the ends
	let start = max(range1.start, range2.start);
	let end = min(range1.end, range2.end);
	if !(start < end) {
		return None;
	}
	Some(Range::new(start, end))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	#[should_panic(expected="start must be < end; got start=3, end=3")]
	fn test_bad_range_1() {
		Range::new(3, 3);
	}

	#[test]
	#[should_panic(expected="start must be < end; got start=3, end=2")]
	fn test_bad_range_2() {
		Range::new(3, 2);
	}

	#[test]
	fn test_intersect() {
		assert_eq!(intersect(Range::new(0, 100), Range::new(0, 100)), Some(Range::new(0, 100)));
		assert_eq!(intersect(Range::new(0, 1), Range::new(0, 2)), Some(Range::new(0, 1)));
		assert_eq!(intersect(Range::new(0, 100), Range::new(1, 100)), Some(Range::new(1, 100)));
		assert_eq!(intersect(Range::new(0, 100), Range::new(50, 150)), Some(Range::new(50, 100)));
		assert_eq!(intersect(Range::new(50, 150), Range::new(0, 100)), Some(Range::new(50, 100)));
		assert_eq!(intersect(Range::new(100, 200), Range::new(50, 150)), Some(Range::new(100, 150)));
		assert_eq!(intersect(Range::new(50, 150), Range::new(100, 200)), Some(Range::new(100, 150)));
		assert_eq!(intersect(Range::new(200, 300), Range::new(50, 150)), None);
		assert_eq!(intersect(Range::new(50, 150), Range::new(200, 300)), None);
	}
}