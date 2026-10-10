use crate::messaging::partial_message::PartialMessage;
use compact_str::CompactString;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::str::Utf8Error;

/// Driver id limit in bytes. Same inline budget as sensor ids (`compact_str` / `String` layout).
pub const MAX_DRIVER_ID_BYTES: usize = 24;
const _: () = assert!(MAX_DRIVER_ID_BYTES == 24);

pub enum CoreControlMessage {
    CHDRV(ChangeDriverMessage),
    SHUTDOWN,
    UNKNOWN(String),
}

impl fmt::Display for CoreControlMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CoreControlMessage::CHDRV(msg) => write!(f, "CHDRV:{}", msg.new_driver_id),
            CoreControlMessage::SHUTDOWN => write!(f, "SHUTDOWN"),
            CoreControlMessage::UNKNOWN(key) => write!(f, "UNKNOWN:{key}"),
        }
    }
}
pub struct ChangeDriverMessage {
    pub new_driver_id: CompactString,
}

#[derive(Debug)]
pub struct MessageParseError {
    pub message: PartialMessage,
    pub kind: MessageParseErrorKind,
}

#[derive(Debug)]
pub enum MessageParseErrorKind {
    MissingKey,
    MissingPayload,
    InvalidPayload(Utf8Error),
    InvalidDriverId(InvalidDriverId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidDriverId {
    Empty,
    TooLong,
    ControlCharacter,
    LineProtocolSpecial,
}

impl Error for MessageParseErrorKind {}

impl fmt::Display for MessageParseErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            MessageParseErrorKind::MissingKey => write!(f, "Message key is missing"),
            MessageParseErrorKind::MissingPayload => write!(f, "Message payload is missing"),
            MessageParseErrorKind::InvalidPayload(decoding_error) => {
                write!(f, "Failed to decode payload: {decoding_error}")
            }
            MessageParseErrorKind::InvalidDriverId(reason) => {
                write!(f, "Invalid driver id: {reason}")
            }
        }
    }
}

impl fmt::Display for InvalidDriverId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            InvalidDriverId::Empty => write!(f, "empty"),
            InvalidDriverId::TooLong => {
                write!(f, "longer than {MAX_DRIVER_ID_BYTES} bytes")
            }
            InvalidDriverId::ControlCharacter => write!(f, "contains ASCII control character"),
            InvalidDriverId::LineProtocolSpecial => {
                write!(f, "contains line-protocol special character (, = space \\\\ \")")
            }
        }
    }
}

/// Rejects values unsafe as `InfluxDB` line-protocol tag values (unescaped).
pub fn validate_driver_id(driver_id: &str) -> Result<(), InvalidDriverId> {
    if driver_id.is_empty() {
        return Err(InvalidDriverId::Empty);
    }
    if driver_id.len() > MAX_DRIVER_ID_BYTES {
        return Err(InvalidDriverId::TooLong);
    }
    for byte in driver_id.bytes() {
        if byte < 0x20 || byte == 0x7f {
            return Err(InvalidDriverId::ControlCharacter);
        }
        if matches!(byte, b',' | b'=' | b' ' | b'\\' | b'"') {
            return Err(InvalidDriverId::LineProtocolSpecial);
        }
    }
    Ok(())
}

impl TryFrom<PartialMessage> for CoreControlMessage {
    type Error = MessageParseError;

    fn try_from(message: PartialMessage) -> Result<Self, Self::Error> {
        let Some(key_bytes) = &message.key else {
            return Err(MessageParseError {
                message,
                kind: MessageParseErrorKind::MissingKey,
            });
        };

        match key_bytes.as_ref() {
            b"CHDRV" => {
                let Some(payload_bytes) = message.payload.as_deref() else {
                    return Err(MessageParseError {
                        message,
                        kind: MessageParseErrorKind::MissingPayload,
                    });
                };
                match CompactString::from_utf8(payload_bytes) {
                    Ok(driver_id) => match validate_driver_id(driver_id.as_str()) {
                        Ok(()) => Ok(CoreControlMessage::CHDRV(ChangeDriverMessage {
                            new_driver_id: driver_id,
                        })),
                        Err(reason) => Err(MessageParseError {
                            message,
                            kind: MessageParseErrorKind::InvalidDriverId(reason),
                        }),
                    },
                    Err(decoding_error) => Err(MessageParseError {
                        message,
                        kind: MessageParseErrorKind::InvalidPayload(decoding_error),
                    }),
                }
            }
            b"SHUTDOWN" => Ok(CoreControlMessage::SHUTDOWN),
            unknown => Ok(CoreControlMessage::UNKNOWN(
                String::from_utf8_lossy(unknown).into_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(key: Option<&[u8]>, payload: Option<&[u8]>) -> PartialMessage {
        PartialMessage {
            key: key.map(Box::from),
            offset: 7,
            partition: 2,
            payload: payload.map(Box::from),
            origin: crate::messaging::partial_message::MessageOrigin::Control,
        }
    }

    #[test]
    fn parses_change_driver_message() {
        let parsed = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(b"driver-7")))
            .expect("CHDRV should parse");

        assert!(
            matches!(parsed, CoreControlMessage::CHDRV(body) if body.new_driver_id == "driver-7")
        );
    }

    #[test]
    fn parses_shutdown_without_payload() {
        let parsed = CoreControlMessage::try_from(message(Some(b"SHUTDOWN"), None))
            .expect("SHUTDOWN should parse");

        assert!(matches!(parsed, CoreControlMessage::SHUTDOWN));
    }

    #[test]
    fn rejects_missing_key_and_payload() {
        let missing_key = CoreControlMessage::try_from(message(None, None));
        assert!(missing_key.is_err());
        let missing_key = missing_key.err().expect("missing key should have an error");
        assert!(matches!(
            missing_key.kind,
            MessageParseErrorKind::MissingKey
        ));

        let missing_payload = CoreControlMessage::try_from(message(Some(b"CHDRV"), None));
        assert!(missing_payload.is_err());
        let missing_payload = missing_payload
            .err()
            .expect("missing payload should have an error");
        assert!(matches!(
            missing_payload.kind,
            MessageParseErrorKind::MissingPayload
        ));
    }

    #[test]
    fn rejects_invalid_driver_id_utf8() {
        let result = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(&[0xff])));
        assert!(result.is_err());
        let error = result.err().expect("invalid UTF-8 should have an error");

        assert!(matches!(
            error.kind,
            MessageParseErrorKind::InvalidPayload(_)
        ));
    }

    #[test]
    fn preserves_unknown_key_as_lossy_string() {
        let parsed = CoreControlMessage::try_from(message(Some(&[b'X', 0xff]), None))
            .expect("unknown keys should be accepted");

        assert!(matches!(parsed, CoreControlMessage::UNKNOWN(value) if value == "X�"));
    }

    #[test]
    fn validate_driver_id_accepts_safe_values() {
        assert!(validate_driver_id("driver-7").is_ok());
        assert!(validate_driver_id("a").is_ok());
        assert!(validate_driver_id(&"x".repeat(MAX_DRIVER_ID_BYTES)).is_ok());
    }

    #[test]
    fn validate_driver_id_rejects_empty() {
        assert_eq!(validate_driver_id(""), Err(InvalidDriverId::Empty));
        let err = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(b"")))
            .err()
            .expect("empty driver id");
        assert!(matches!(
            err.kind,
            MessageParseErrorKind::InvalidDriverId(InvalidDriverId::Empty)
        ));
    }

    #[test]
    fn validate_driver_id_rejects_too_long() {
        let long = "x".repeat(MAX_DRIVER_ID_BYTES + 1);
        assert_eq!(
            validate_driver_id(&long),
            Err(InvalidDriverId::TooLong)
        );
        let err = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(long.as_bytes())))
            .err()
            .expect("oversized driver id");
        assert!(matches!(
            err.kind,
            MessageParseErrorKind::InvalidDriverId(InvalidDriverId::TooLong)
        ));
    }

    #[test]
    fn validate_driver_id_rejects_control_characters() {
        assert_eq!(
            validate_driver_id("bad\ndriver"),
            Err(InvalidDriverId::ControlCharacter)
        );
        assert_eq!(
            validate_driver_id("bad\x7fdriver"),
            Err(InvalidDriverId::ControlCharacter)
        );
        let err = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(b"bad\tdriver")))
            .err()
            .expect("control char driver id");
        assert!(matches!(
            err.kind,
            MessageParseErrorKind::InvalidDriverId(InvalidDriverId::ControlCharacter)
        ));
    }

    #[test]
    fn validate_driver_id_rejects_line_protocol_specials() {
        for bad in ["x,race=other", "has space", "eq=bad", "slash\\y", "quote\"d"] {
            assert_eq!(
                validate_driver_id(bad),
                Err(InvalidDriverId::LineProtocolSpecial),
                "expected reject for {bad:?}"
            );
        }
        let err = CoreControlMessage::try_from(message(Some(b"CHDRV"), Some(b"x,race=other")))
            .err()
            .expect("line-protocol special");
        assert!(matches!(
            err.kind,
            MessageParseErrorKind::InvalidDriverId(InvalidDriverId::LineProtocolSpecial)
        ));
    }
}
