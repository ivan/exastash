//! Functions to check filenames for Windows compatibility

/// Reasons why a path segment cannot be used on Windows.
#[expect(variant_size_differences)]
#[derive(Debug, Eq, thiserror::Error, PartialEq)]
pub enum PathError {
    #[error("the path contains the character {0:?}, which is not allowed on Windows")]
    ContainsInvalidWindowsCharacter(char),

    #[error("the path ends with {0:?}, which is not allowed on Windows")]
    InvalidWindowsNameEnding(char),

    #[error("the name {0:?} is a reserved device name on Windows")]
    ReservedWindowsDeviceName(&'static str),
}

fn check_windows_special_characters(segment: &str) -> Result<(), PathError> {
    for c in segment.chars() {
        if matches!(c, '"' | '*' | ':' | '<' | '>' | '?' | '\\' | '|' | '\0'..='\x1F') {
            return Err(PathError::ContainsInvalidWindowsCharacter(c));
        }
    }
    Ok(())
}

fn check_windows_segment_ending(segment: &str) -> Result<(), PathError> {
    if segment.ends_with('.') {
        Err(PathError::InvalidWindowsNameEnding('.'))
    } else if segment.ends_with(' ') {
        Err(PathError::InvalidWindowsNameEnding(' '))
    } else {
        Ok(())
    }
}

fn check_windows_device_name(segment: &str) -> Result<(), PathError> {
    let before_dot = match segment.split('.').next() {
        Some(s) => s,
        None => return Ok(()),
    };
    match before_dot.len() {
        3 => {
            let mut name: [u8; 3] = [0; 3];
            name.clone_from_slice(before_dot.as_bytes());
            name.make_ascii_lowercase();
            match &name {
                b"aux" => Err(PathError::ReservedWindowsDeviceName("AUX")),
                b"con" => Err(PathError::ReservedWindowsDeviceName("CON")),
                b"nul" => Err(PathError::ReservedWindowsDeviceName("NUL")),
                b"prn" => Err(PathError::ReservedWindowsDeviceName("PRN")),
                _      => Ok(()),
            }
        }
        4 => {
            let mut name: [u8; 4] = [0; 4];
            name.clone_from_slice(before_dot.as_bytes());
            name.make_ascii_lowercase();
            match &name {
                // https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file
                // neglects to mention COM0 and LPT0, but explorer does choke on them,
                // on Windows 7 - 10 (tested 20H2).
                b"com0" => Err(PathError::ReservedWindowsDeviceName("COM0")),
                b"com1" => Err(PathError::ReservedWindowsDeviceName("COM1")),
                b"com2" => Err(PathError::ReservedWindowsDeviceName("COM2")),
                b"com3" => Err(PathError::ReservedWindowsDeviceName("COM3")),
                b"com4" => Err(PathError::ReservedWindowsDeviceName("COM4")),
                b"com5" => Err(PathError::ReservedWindowsDeviceName("COM5")),
                b"com6" => Err(PathError::ReservedWindowsDeviceName("COM6")),
                b"com7" => Err(PathError::ReservedWindowsDeviceName("COM7")),
                b"com8" => Err(PathError::ReservedWindowsDeviceName("COM8")),
                b"com9" => Err(PathError::ReservedWindowsDeviceName("COM9")),
                b"lpt0" => Err(PathError::ReservedWindowsDeviceName("LPT0")),
                b"lpt1" => Err(PathError::ReservedWindowsDeviceName("LPT1")),
                b"lpt2" => Err(PathError::ReservedWindowsDeviceName("LPT2")),
                b"lpt3" => Err(PathError::ReservedWindowsDeviceName("LPT3")),
                b"lpt4" => Err(PathError::ReservedWindowsDeviceName("LPT4")),
                b"lpt5" => Err(PathError::ReservedWindowsDeviceName("LPT5")),
                b"lpt6" => Err(PathError::ReservedWindowsDeviceName("LPT6")),
                b"lpt7" => Err(PathError::ReservedWindowsDeviceName("LPT7")),
                b"lpt8" => Err(PathError::ReservedWindowsDeviceName("LPT8")),
                b"lpt9" => Err(PathError::ReservedWindowsDeviceName("LPT9")),
                _       => Ok(()),
            }
        }
        _ => Ok(())
    }
}

/// Check whether a UTF-8 path segment is valid without doing any validation that
/// would be redundant with path-normalization code or the CHECK in PostgreSQL.
pub(crate) fn check_segment(segment: &str) -> Result<(), PathError> {
    check_windows_special_characters(segment)?;
    check_windows_segment_ending(segment)?;
    check_windows_device_name(segment)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_segment() {
        assert_eq!(check_segment("filename"), Ok(()));
        assert_eq!(check_segment("filename.ext"), Ok(()));
        assert_eq!(check_segment("with spaces"), Ok(()));
        assert_eq!(check_segment("multiple.ext.ext"), Ok(()));

        assert_eq!(check_segment("with CR\r"), Err(PathError::ContainsInvalidWindowsCharacter('\r')));
        assert_eq!(check_segment("ends with dot."), Err(PathError::InvalidWindowsNameEnding('.')));
        assert_eq!(check_segment("ends with space "), Err(PathError::InvalidWindowsNameEnding(' ')));

        let mut invalid_chars = vec!['"', '*', ':', '<', '>', '?', '\\', '|'];
        for c in '\0'..'\x1F' {
            invalid_chars.push(c);
        }
        for invalid in invalid_chars.iter() {
            assert_eq!(check_segment(&format!("{}", invalid)), Err(PathError::ContainsInvalidWindowsCharacter(*invalid)));
        }

        let devices = [
            "AUX", "CON", "NUL", "PRN",
            "COM0", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT0", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
        ];
        for device in devices.iter() {
            assert_eq!(check_segment(device),                       Err(PathError::ReservedWindowsDeviceName(device)));
            assert_eq!(check_segment(&format!("{}.c", device)),     Err(PathError::ReservedWindowsDeviceName(device)));
            assert_eq!(check_segment(&format!("{}.c.old", device)), Err(PathError::ReservedWindowsDeviceName(device)));

            assert_eq!(check_segment(&format!("{}{}", device, device)), Ok(()));
            assert_eq!(check_segment(&format!("c.{}", device)), Ok(()));
        }
    }
}
