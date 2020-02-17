use std::time::Duration;
use std::cmp::min;
use std::ops::Mul;
use num::rational::Ratio;

// We need to be able to multiply a std::time::Duration by a
// num::rational:Ratio, so we need our own Mul trait.
pub(crate) trait MyMul<RHS=Self> {
    type Output;
    fn mymul(self, rhs: RHS) -> Self::Output;
}

impl MyMul<u32> for Duration {
    type Output = Duration;
    fn mymul(self, rhs: u32) -> Duration {
        self * rhs
    }
}

impl MyMul<Ratio<u32>> for Duration {
    type Output = Duration;
    fn mymul(self, rhs: Ratio<u32>) -> Duration {
        (self * *rhs.numer()) / *rhs.denom()
    }
}

// Implement MyMul for Mul's
impl<T> MyMul for T where T: Mul<Output = T> {
    type Output = T;
    fn mymul(self, rhs: T) -> T {
        self * rhs
    }
}

pub(crate) struct Decayer<N: MyMul<M, Output=N> + Ord + Copy, M: Copy> {
    /// initial number to return
    initial: N,
    /// multiply number by this value after each call to decay()
    multiplier: M,
    /// cap number at this value
    max: N,
    /// current number
    current: N,
    /// still on first value?
    first: bool
}

impl <N: MyMul<M, Output=N> + Ord + Copy, M: Copy> Decayer<N, M> {
    pub(crate) fn new(initial: N, multiplier: M, max: N) -> Decayer<N, M> {
        Decayer {
            initial,
            multiplier,
            max,
            first: true,
            current: initial,
        }
    }

    pub(crate) fn reset(&mut self) -> N {
        self.first = true;
        self.current = self.initial;
        self.current
    }

    pub(crate) fn decay(&mut self) -> N {
        if self.first {
            self.first = false;
        } else {
            self.current = min(self.current.mymul(self.multiplier), self.max);
        }
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use num::rational::Ratio;

    #[test]
    fn test_decayer_u32() {
        let mut decayer: Decayer<u32, u32> = Decayer::new(1, 2, 20);
        assert_eq!(decayer.decay(), 1);
        assert_eq!(decayer.decay(), 2);
        assert_eq!(decayer.decay(), 4);
        assert_eq!(decayer.decay(), 8);
        assert_eq!(decayer.decay(), 16);
        assert_eq!(decayer.decay(), 20);
        assert_eq!(decayer.decay(), 20);
        assert_eq!(decayer.reset(), 1);
    }

    #[test]
    fn test_decayer_u64() {
        let mut decayer: Decayer<u64, u64> = Decayer::new(1u64, 2u64, 20u64);
        assert_eq!(decayer.decay(), 1);
        assert_eq!(decayer.decay(), 2);
        assert_eq!(decayer.decay(), 4);
        assert_eq!(decayer.decay(), 8);
        assert_eq!(decayer.decay(), 16);
        assert_eq!(decayer.decay(), 20);
        assert_eq!(decayer.decay(), 20);
        assert_eq!(decayer.reset(), 1);
    }

    #[test]
    fn test_decayer_duration() {
        let mut decayer: Decayer<Duration, Ratio<u32>> = Decayer::new(
            Duration::new(1, 0),
            Ratio::new(3, 2),
            Duration::new(4, 0)
        );
        assert_eq!(decayer.decay(), Duration::new(1, 0));
        assert_eq!(decayer.decay(), Duration::new(1, 500000000));
        assert_eq!(decayer.decay(), Duration::new(2, 250000000));
        assert_eq!(decayer.decay(), Duration::new(3, 375000000));
        assert_eq!(decayer.decay(), Duration::new(4, 0));
        assert_eq!(decayer.decay(), Duration::new(4, 0));
        assert_eq!(decayer.reset(), Duration::new(1, 0));
    }
}
