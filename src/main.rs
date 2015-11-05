extern crate rustc_serialize;
extern crate docopt;
extern crate toml;

use docopt::Docopt;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use rustc_serialize::serialize::Decodable;

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

#[derive(Debug, RustcDecodable)]
struct Stash {
	name: String,
	paths: Vec<String>,
	db: String
}

#[derive(Debug, RustcDecodable)]
struct Config {
	stashes: Vec<Stash>
}

fn get_config() -> Result<Config, Box<Error>> {
	let mut config_file = try!(File::open("/home/at/.config/rs-terastash.toml"));
	let mut config_content = String::new();
	try!(config_file.read_to_string(&mut config_content));
	let mut parser = toml::Parser::new(&config_content);
	let toml = parser.parse().unwrap();
	let mut decoder = toml::Decoder::new(toml);
	let config = try!(Config::decode(&mut decoder));
	Ok(config)
}

fn main() {
	let args: Args = Docopt::new(USAGE)
		.and_then(|d| d.decode())
		.unwrap_or_else(|e| e.exit());
	println!("{:?}", args);
	get_config()
}
