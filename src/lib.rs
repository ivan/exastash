#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(regex_macros)]

extern crate regex;
extern crate num;

pub mod conceal_size;
pub mod universal_filename;
pub mod ranges;
pub mod retry;
