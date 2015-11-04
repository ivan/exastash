extern crate rustc_serialize;
extern crate docopt;

use docopt::Docopt;

const USAGE: &'static str = "
terastash

Usage:
	ts init -n <name>
	ts (-h | --help)
	ts --version

Options:
	-h --help     Show this screen.
	--version     Show version.
";

#[derive(Debug, RustcDecodable)]
struct Args {
	arg_name: String,
	cmd_init: bool
}

fn main() {
	let args: Args = Docopt::new(USAGE)
		.and_then(|d| d.decode())
		.unwrap_or_else(|e| e.exit());
	println!("{:?}", args);
}
