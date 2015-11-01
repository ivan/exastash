use std::collections::HashSet;

#[derive(Display, Debug, Eq, PartialEq)]
pub struct BadFilenameError {
	message: String
}

/**
 * Checks that a unicode basename is legal on Windows, Linux, and OS X.
 * If it isn't, return `BadFilenameError`.
 */
pub fn check(s: &String) -> Result<(), BadFilenameError> {
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
	if regex!(r"^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$").is_match(s) {
		return Err(BadFilenameError { message: "Some Windows APIs do not support filenames ` +
			`whose non-extension component is ${inspect(first_part)}; got ${inspect(s)}".to_owned() });
	}
	// TODO \\  \?
	/*if regex!(r"[\|<>:\"/\*\x00-\x1F]").is_match(s) {
		return Err(BadFilenameError { message: "Windows does not support filenames that contain " +
			"\\x00-\\x1F or any of: | < > : " / \\ ? *; got ${inspect(s)}".to_owned() });
	}*/
	if s.len() > 255 {
		return Err(BadFilenameError { message: "Windows does not support filenames with > 255 characters; ${inspect(s)} has ${s.length}".to_owned() });
	}
	let bytes_len = s.into_bytes().len();
	if bytes_len > 255 {
		return Err(BadFilenameError { message: "Linux does not support filenames with > 255 bytes; ${inspect(s)} has ${bytes_len}".to_owned() });
	}
	Ok(())
}

#[test]
fn test_valid_filenames() {
	assert!(check("hello") == Ok(()));
}
