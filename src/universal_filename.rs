#[derive(Debug, Eq, PartialEq)]
pub struct BadFilenameError {
	message: String
}

pub type CheckResult = Result<(), BadFilenameError>;

/**
 * Checks that a unicode basename is legal on Windows, Linux, and OS X.
 * If it isn't, return `BadFilenameError`.
 */
pub fn check(s: &str) -> CheckResult {
	if regex!(r"\x00").is_match(s) {
		return Err(BadFilenameError { message: "Filename cannot contain NULL; got ${inspect(s)}".to_owned() });
	}
	if regex!(r"/").is_match(s) {
		return Err(BadFilenameError { message: "Filename cannot contain '/'; got ${inspect(s)}".to_owned() });
	}
	let trimmed = s.trim();
	if trimmed == "" || trimmed == "." || trimmed == ".." {
		return Err(BadFilenameError { message: "Trimmed filename cannot be '', '.', or '..'; got ${inspect(trimmed)} from ${inspect(s)}".to_owned() });
	}
	if regex!(r"\.$").is_match(s) {
		return Err(BadFilenameError { message: "Windows shell does not support filenames that end with '.'; got ${inspect(s)}".to_owned() });
	}
	if regex!(r" $").is_match(s) {
		return Err(BadFilenameError { message: "Windows shell does not support filenames that end with space; got ${inspect(s)}".to_owned() });
	}
	let first_part = s.split(".").next().unwrap().to_uppercase();
	if regex!(r"^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$").is_match(&first_part) {
		return Err(BadFilenameError { message: "Some Windows APIs do not support filenames \
			whose non-extension component is ${inspect(first_part)}; got ${inspect(s)}".to_owned() });
	}
	if regex!(r#"[\|<>:"/\?\*\\\x00-\x1F]"#).is_match(s) {
		return Err(BadFilenameError { message: "Windows does not support filenames that contain \
			\\x00-\\x1F or any of: | < > : \" / \\ ? *; got ${inspect(s)}".to_owned() });
	}
	// We don't need to check the character length of a filename, because if it
	// has > 255 characters, it also has > 255 bytes.
	if s.len() > 255 {
		return Err(BadFilenameError { message: "Linux does not support filenames with > 255 bytes; ${inspect(s)} has ${bytes_len}".to_owned() });
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::iter::repeat;
	use regex::Regex;

	fn assert_error_with_message(val: CheckResult, pat: Regex) {
		match val {
			Ok(_) => panic!("Expected Err, got Ok"),
			Err(BadFilenameError { message }) => {
				if !pat.is_match(&message) {
					panic!(format!("Error did not match '{:?}', was {:?}", pat, message));
				}
			}
		}
	}

	#[test]
	fn test_invalid_filenames() {
		// We avoid using regex! here to save on compile time.
		// 38 regex! macros leads to +16 seconds compile and +3MB binary size
		// (tested: Rust nightly 2015-10-29 on a 4790K)
		assert_error_with_message(check("x/y"),            Regex::new(r"cannot contain '/'").unwrap());
		assert_error_with_message(check("x\x00y"),         Regex::new(r"cannot contain NULL").unwrap());
		assert_error_with_message(check(""),               Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check("."),              Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check(".."),             Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check(" "),              Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check(" . "),            Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check(" .. "),           Regex::new(r"cannot be '', '\.', or '\.\.'").unwrap());
		assert_error_with_message(check("hello."),         Regex::new(r"^Windows shell does not support filenames that end with '\.'").unwrap());
		assert_error_with_message(check("hello "),         Regex::new(r"^Windows shell does not support filenames that end with space").unwrap());
		assert_error_with_message(check("con"),            Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("con.c"),          Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("con.c.last"),     Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("COM7"),           Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("COM7.c"),         Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("COM7.c.last"),    Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("lpt9"),           Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("lpt9.c"),         Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("lpt9.c.last"),    Regex::new(r"not support filenames whose non-extension component is ").unwrap());
		assert_error_with_message(check("hello\\world"),   Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello:world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello?world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello>world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello<world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello|world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello\"world"),   Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello*world"),    Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello\x01world"), Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello\nworld"),   Regex::new(r"not support filenames that contain ").unwrap());
		assert_error_with_message(check("hello\x1Fworld"), Regex::new(r"not support filenames that contain ").unwrap());
		let s_256 = repeat("\u{cccc}").take(256).collect::<String>();
		assert_eq!(s_256.chars().count(), 256);
		assert_error_with_message(check(&s_256),           Regex::new(r"not support filenames with > 255 bytes").unwrap());
		let s_128 = repeat("\u{cccc}").take(128).collect::<String>();
		assert_eq!(s_128.chars().count(), 128);
		assert_error_with_message(check(&s_128),           Regex::new(r"not support filenames with > 255 bytes").unwrap());
	}

	#[test]
	fn test_valid_filenames() {
		assert_eq!(check("hello"), Ok(()));
		assert_eq!(check("hello world"), Ok(()));
		assert_eq!(check(". .x"), Ok(()));
		assert_eq!(check("#'test'"), Ok(()));
		assert_eq!(check("hello\u{cccc}world"), Ok(()));
		let long_string = repeat("h").take(255).collect::<String>();
		assert_eq!(check(&long_string), Ok(()));
	}
}