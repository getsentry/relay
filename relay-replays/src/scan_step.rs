use std::fmt::Display;

// UTF-8 decimal value for special JSON control characters
const OPEN_BRACKET: u8 = 123;
const CLOSE_BRACKET: u8 = 125;
const LINE_FEED: u8 = 10;
const CARRIAGE_RETURN: u8 = 13;
const SPACE: u8 = 32;
const QUOTE: u8 = 34;
const COMMA: u8 = 44;
const COLON: u8 = 58;
const DIGIT_FIVE: u8 = 53;

fn transform_breadcrumbs(bytes: &[u8]) -> Result<Vec<u8>, ScanError> {
    // Get the (start, end) positions of each breadcrumb event.
    let positions = get_breadcrumb_slice_ranges(bytes)?;

    // Store the transformed output in a new buffer.
    let mut buffer: Vec<u8> = Vec::new();
    fill_buffer(&mut buffer, bytes, positions);

    // Success.
    Ok(buffer)
}

fn fill_buffer(buffer: &mut Vec<u8>, bytes: &[u8], positions: Vec<(usize, usize)>) {
    match positions[..] {
        [(start, end), ..] => {
            let (prefix, rest) = bytes.split_at(start);
            let (breadcrumb, bits) = rest.split_at(end);

            buffer.extend(prefix);
            buffer.extend(scrub_pii(breadcrumb));

            let (_, rest) = positions.split_at(1);
            fill_buffer(buffer, bits, rest.to_owned())
        }
        // The empty case accepts the contents of bytes and pushes it into
        // the output buffer.
        [] => buffer.extend(bytes),
    }
}

fn scrub_pii(bytes: &[u8]) -> Vec<u8> {
    bytes.to_owned()
}

fn get_breadcrumb_slice_ranges(bytes: &[u8]) -> Result<Vec<(usize, usize)>, ScanError> {
    let mut i: usize = 0;
    let mut result: Vec<(usize, usize)> = Vec::new();

    while i < bytes.len() {
        if bytes[i] == OPEN_BRACKET {
            let (end, is_breadcrumb) = find_closing_bracket(i, bytes)?;

            // Only push the range into the buffer if it is a custom event
            // type.  RRWeb events are not currently parsed.
            if is_breadcrumb {
                result.push((i, end));
            }

            // Move the pointer to the end of the range to prevent iterating
            // over the same bytes more than once.
            i = end;
        }
        i += 1;
    }

    Ok(result)
}

// Find the closing bracket in the byte sequence.  Bracket nesting ishandled
// by a bracket_depth counter.  We do not validate if the brackets match or
// if they make well formed JSON.  Those steps come later in the pipeline.
// This is a quick check to determine the types we need to pay close
// attention to.
fn find_closing_bracket(start: usize, bytes: &[u8]) -> Result<(usize, bool), ScanError> {
    let mut i = start;
    let mut bracket_depth: usize = 0;
    let mut is_breadcrumb: bool = false;

    while i < bytes.len() {
        if bytes[i] == CLOSE_BRACKET {
            bracket_depth -= 1;
            if bracket_depth == 0 {
                return Ok((i, is_breadcrumb));
            }
        } else if bytes[i] == OPEN_BRACKET {
            bracket_depth += 1;
        } else if is_breadcrumb == false && bracket_depth == 1 && bytes[i] == QUOTE {
            is_breadcrumb = custom_event_scanner(bytes, i);
        }
        i += 1
    }

    Err(ScanError::NoClosingBracket)
}

// Partial implementation of a JSON scanner. We don't care about the
// integrity of the JSON we only care about the minimum problem scope of
// finding the following sequence `"type":5`. We follow the minimum number
// of JSON rules to guess that this is a breadcrumb event. We will determine
// the validity in a post-processing step.
fn custom_event_scanner(bytes: &[u8], mut i: usize) -> bool {
    let prefix = "\"type\"".as_bytes();

    for c in prefix {
        if bytes[i] == *c {
            i += 1
        } else {
            return false;
        }
    }

    i = spaces(bytes, i);

    // Detect ":" character.
    if i >= bytes.len() || bytes[i] != COLON {
        return false;
    }

    i += 1; // len of `:`
    i = spaces(bytes, i);

    // Detect "5" character.
    if i >= bytes.len() || bytes[i] != DIGIT_FIVE {
        return false;
    }

    i += 1; // len of `5`
    i = spaces(bytes, i);

    // Detect ending sequence (",", "}")
    if i >= bytes.len() {
        return false;
    } else if bytes[i] == COMMA || bytes[i] == CLOSE_BRACKET {
        return true;
    }

    false
}

// Return the next byte if possible.
fn peek(bytes: &[u8], i: usize) -> Option<u8> {
    let next = i + 1;

    if bytes.len() > next {
        Some(bytes[next])
    } else {
        None
    }
}

// Increment the index position until you stop seeing space characters. If
// the loop break then we return an index position equal to buffer.len().
fn spaces(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        if bytes[i] == SPACE {
            i += 1
        } else if let Some(n) = new_line(bytes, i) {
            // New line characters increment by 1 or 2 bytes.
            i += n
        } else {
            // If not a new line sequence or a space we terminate the loop
            // and return the current index position.
            break;
        }
    }
    i
}

// Return an optional offset to skip the new-line characters.
fn new_line(bytes: &[u8], i: usize) -> Option<usize> {
    if bytes[i] == LINE_FEED {
        return Some(1); // LF for sane operating systems.
    } else if bytes[i] == CARRIAGE_RETURN {
        if peek(bytes, i) == Some(LINE_FEED) {
            return Some(2); // CRLF for Windows based machines.
        }
    }
    None
}

#[derive(Debug)]
pub enum ScanError {
    NoClosingBracket,
    NoClosingQuote,
}

impl Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanError::NoClosingBracket => write!(f, "missing closing bracket"),
            ScanError::NoClosingQuote => write!(f, "missing closing quote"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::{
        custom_event_scanner, find_closing_bracket, get_breadcrumb_slice_ranges,
        transform_breadcrumbs,
    };

    #[test]
    fn test_custom_event_scanner() {
        // No spaces or new lines.
        assert!(custom_event_scanner(b"\"type\": 5}", 0));
        assert!(custom_event_scanner(b"\"type\": 5,", 0));
        // Spaces
        assert!(custom_event_scanner(b"\"type\": 5  ,", 0));
        assert!(custom_event_scanner(b"\"type\": 5  }", 0));
        // New lines.
        assert!(custom_event_scanner(b"\"type\": 5\n\n,", 0));
        assert!(custom_event_scanner(b"\"type\": 5\n\n}", 0));
        // Spaces and new lines.
        assert!(custom_event_scanner(b"\"type\": 5 \n \n,", 0));
        assert!(custom_event_scanner(b"\"type\": 5 \n \n}", 0));
        // Spaces and carriage return new lines.
        assert!(custom_event_scanner(b"\"type\": 5 \r\n \r\n,", 0));
        assert!(custom_event_scanner(b"\"type\": 5 \r\n \r\n}", 0));

        // Fails to parse
        assert!(!custom_event_scanner(b"\"type\": 55}", 0));
        assert!(!custom_event_scanner(b"\"other\": 5}", 0));
        assert!(!custom_event_scanner(b"\"type\": 4}", 0));
        assert!(!custom_event_scanner(b"\"type\": false,", 0));
        assert!(!custom_event_scanner(b"\"type\": \"5\",", 0));
    }

    #[test]
    fn test_find_closing_bracket() {
        assert!(find_closing_bracket(0, b"{}").unwrap() == (1, false));
        assert!(find_closing_bracket(0, b"{\"type\":5}").unwrap() == (9, true));
        assert!(find_closing_bracket(0, b"{{{").is_err());
    }

    #[test]
    fn test_get_breadcrumb_slice_ranges() {
        // Breadcrumb event.
        assert!(get_breadcrumb_slice_ranges(b"[{\"type\": 5}]").unwrap() == vec![(1, 11)]);
        // Non-breadcrumb event.
        assert!(get_breadcrumb_slice_ranges(b"[{\"type\": 3}]").unwrap() == vec![]);
        // No opinion on the structure of the JSON.
        assert!(get_breadcrumb_slice_ranges(b"{\"type\": 5}").unwrap() == vec![(0, 10)]);
        // Missing brackets will raise an error however.
        assert!(get_breadcrumb_slice_ranges(b"{").is_err());
    }

    #[test]
    fn tes_transform_breadcrumbs() {
        let bytes = r#"[{"type":5},{"type":3}]"#.as_bytes();
        let output = transform_breadcrumbs(bytes).unwrap();
        assert!(output == bytes.to_owned());
    }
}
