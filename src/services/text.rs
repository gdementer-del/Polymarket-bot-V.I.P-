//! Text cleanup helpers for operator-facing diagnostics.

use std::io::{self, Write};

use tracing_subscriber::fmt::MakeWriter;

const MOJIBAKE_MARKERS: &[&str] = &[
    "\u{420}\u{a0}",
    "\u{420}\u{40e}",
    "\u{420}\u{40b}",
    "\u{420}\u{2019}",
    "\u{420}\u{406}",
    "\u{420}\u{45f}",
    "\u{420}\u{a4}",
    "\u{412}\u{a0}",
    "\u{432}\u{402}",
    "\u{421}\u{403}",
    "\u{421}\u{201a}",
    "\u{421}\u{40a}",
    "\u{421}\u{2021}",
    "\u{421}\u{20ac}",
    "\u{421}\u{2039}",
    "\u{421}\u{40c}",
    "\u{421}\u{40b}",
    "\u{421}\u{40f}",
];

/// Detect legacy double-encoded Russian text that used to leak into logs.
#[must_use]
pub fn contains_legacy_mojibake(value: &str) -> bool {
    MOJIBAKE_MARKERS.iter().any(|marker| value.contains(marker))
}

/// Keep operator output ASCII-safe when old corrupted literals are encountered.
#[must_use]
pub fn sanitize_legacy_mojibake(value: &str) -> String {
    let normalized = normalize_common_labels(value);
    if !contains_legacy_mojibake(&normalized) {
        return normalized;
    }

    let mut output = String::with_capacity(normalized.len());
    let mut previous_was_space = false;

    for character in normalized.chars() {
        if character == '\n' {
            trim_trailing_space(&mut output);
            output.push('\n');
            previous_was_space = true;
        } else if character.is_ascii_graphic() {
            output.push(character);
            previous_was_space = false;
        } else if !previous_was_space {
            output.push(' ');
            previous_was_space = true;
        }
    }

    let cleaned = output.lines().map(str::trim).collect::<Vec<_>>().join("\n");
    if cleaned.trim().is_empty() {
        "[encoding-corrupt-text]".to_owned()
    } else {
        cleaned
    }
}

pub struct SanitizingStderr;

impl<'writer> MakeWriter<'writer> for SanitizingStderr {
    type Writer = SanitizingWriter<io::Stderr>;

    fn make_writer(&'writer self) -> Self::Writer {
        SanitizingWriter::new(io::stderr())
    }
}

pub struct SanitizingWriter<W: Write> {
    inner: W,
    buffer: Vec<u8>,
}

impl<W: Write> SanitizingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
        }
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return self.inner.flush();
        }

        let text = String::from_utf8_lossy(&self.buffer);
        let cleaned = sanitize_legacy_mojibake(&text);
        self.inner.write_all(cleaned.as_bytes())?;
        self.buffer.clear();
        self.inner.flush()
    }
}

impl<W: Write> Write for SanitizingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()
    }
}

impl<W: Write> Drop for SanitizingWriter<W> {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}

fn normalize_common_labels(value: &str) -> String {
    value
        .replace("\u{420}\u{43e}\u{441}\u{442}", "Up")
        .replace("\u{41f}\u{430}\u{434}\u{435}\u{43d}\u{438}\u{435}", "Down")
        .replace("\u{424}\u{43b}\u{44d}\u{442}", "Flat")
}

fn trim_trailing_space(output: &mut String) {
    while output.ends_with(' ') {
        let _ = output.pop();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::{contains_legacy_mojibake, sanitize_legacy_mojibake};

    const LEGACY_SAMPLE: &str = "edge \u{420}\u{a0}\u{420}\u{2026}\u{420}\u{a0}\u{421}\u{2018}\u{420}\u{a0}\u{412}\u{b6}\u{420}\u{a0}\u{412}\u{b5} threshold";

    #[test]
    fn detects_and_sanitizes_legacy_mojibake() {
        let cleaned = sanitize_legacy_mojibake(LEGACY_SAMPLE);

        assert!(contains_legacy_mojibake("\u{420}\u{a0}\u{420}\u{2026}"));
        assert!(!cleaned.contains('\u{420}'));
        assert!(cleaned.contains("edge"));
        assert!(cleaned.contains("threshold"));
    }

    #[test]
    fn keeps_clean_ascii_unchanged() {
        assert_eq!(
            sanitize_legacy_mojibake("signal is below threshold"),
            "signal is below threshold"
        );
    }

    #[test]
    fn writer_sanitizes_buffered_log_line() {
        let mut output = Vec::new();
        {
            let mut writer = super::SanitizingWriter::new(&mut output);
            writer
                .write_all(LEGACY_SAMPLE.replace("edge", "warning").as_bytes())
                .unwrap();
            writer.flush().unwrap();
        }

        let output = String::from_utf8(output).unwrap();
        assert!(!output.contains('\u{420}'));
        assert!(output.contains("warning"));
        assert!(output.contains("threshold"));
    }
}
