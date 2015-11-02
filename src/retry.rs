use std::time::Duration;
use std::cmp::min;
use num::rational::Ratio;

// We need to be able to multiply a std::time::Duration by a
// num::rational:Ratio, so we need our own Mul trait.
trait MyMul<RHS=Self> {
	type Output;
	fn mul(self, rhs: RHS) -> Self::Output;
}

impl MyMul<u32> for Duration {
	type Output = Duration;
	fn mul(self, rhs: u32) -> Duration {
		self * rhs
	}
}

impl MyMul<Ratio<u32>> for Duration {
	type Output = Duration;
	fn mul(self, rhs: Ratio<u32>) -> Duration {
		(self * rhs.numer) / rhs.denom
	}
}

impl MyMul<u32> for u32 {
	type Output = u32;
	fn mul(self, rhs: u32) -> u32 {
		self * rhs
	}
}

pub struct Decayer<N, M> {
	/// initial number to return
	initial: N,
	/// multiply number by this value after each call to decay()
	multiplier: M,
	/// cap number at this value
	max: N,
	/// current number
	current: N,
}

impl <N, M> Decayer<N, M> {
	pub fn new(initial: N, multiplier: M, max: N) -> Decayer<N, M> {
		let decayer = Decayer {
			initial: initial,
			multiplier: multiplier,
			max: max,
			current: initial,
		};
		decayer.reset();
		decayer
	}

	pub fn reset(&self) {
		// First call to .decay() will multiply, but we want to get the `intitial`
		// value on the first call to .decay(), so divide.
		self.current = MyMul::mul(self.initial, 1 / self.multiplier);
		self.current
	}

	// For use inside an errback where you want to tell the user how many
	// seconds the delay will be.
	pub fn get_next_delay(&self) {
		min(MyMul::mul(self.current * self.multiplier), self.max)
	}

	pub fn decay(&self) {
		self.current = self.get_next_delay();
		self.current
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_decayer() {
		let decayer: Decayer<u32, u32> = Decayer::new(1, 2, 20);
		assert_eq!(decayer.decay(), 1);
		assert_eq!(decayer.decay(), 2);
		assert_eq!(decayer.decay(), 4);
		assert_eq!(decayer.decay(), 8);
		assert_eq!(decayer.decay(), 16);
		assert_eq!(decayer.decay(), 20);
		assert_eq!(decayer.decay(), 20);
		assert_eq!(decayer.reset(), 1);
	}
}
