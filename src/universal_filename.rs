use std::collections::HashSet;

struct BadFilenameError<'a> {
	reason: &'a String
}

static device_names: HashSet<&'static str> = vec!(
	"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6",
	"COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6",
	"LPT7", "LPT8", "LPT9").into_iter().collect();

/**
 * Checks that a unicode basename is legal on Windows, Linux, and OS X.
 * If it isn't, return `BadFilenameError`.
 */
pub fn check(s: &String) -> Result((), String) {
	if regex!(r"\x00").is_match(s) {
		return Err(BadFilenameError { reason: "Filename cannot contain NULL; got ${inspect(s)}" });
	}
	if regex!(r"/").is_match(s) {
		return Err(BadFilenameError { reason: "Filename cannot contain '/'; got ${inspect(s)}" });
	}
	let trimmed = s.trim();
	if trimmed == "" || trimmed == "." || trimmed == ".." {
		return Err(BadFilenameError { reason: "Trimmed filename cannot be '', '.', or '..'; got ${inspect(trimmed)} from ${inspect(s)}" });
	}
	if regex!(r"\.$").is_match(s) {
		return Err(BadFilenameError { reason: "Windows shell does not support filenames that end with '.'; got ${inspect(s)}" });
	}
	if regex!(r" $").is_match(s) {
		return Err(BadFilenameError { reason: "Windows shell does not support filenames that end with space; got ${inspect(s)}" });
	}
	let firstPart = s.split(".")[0].to_uppercase();
	if device_names.contains(firstPart) {
		return Err(BadFilenameError { reason: "Some Windows APIs do not support filenames ` +
			`whose non-extension component is ${inspect(firstPart)}; got ${inspect(s)}" });
	}
	// TODO \\  \?
	/*if regex!(r"[\|<>:\"/\*\x00-\x1F]").is_match(s) {
		return Err(BadFilenameError { reason: "Windows does not support filenames that contain " +
			"\\x00-\\x1F or any of: | < > : " / \\ ? *; got ${inspect(s)}" });
	}*/
	if s.len() > 255 {
		return Err(BadFilenameError { reason: "Windows does not support filenames with > 255 characters; ${inspect(s)} has ${s.length}" });
	}
	let bytes_len = s.into_bytes().len();
	if bytes_len > 255 {
		return Err(BadFilenameError { reason: "Linux does not support filenames with > 255 bytes; ${inspect(s)} has ${bytes_len}" });
	}
	()
}

#[test]
fn test_valid_filenames() {
	assert!(check("hello") == Ok(()));
}
