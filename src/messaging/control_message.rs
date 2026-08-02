use crate::messaging::partial_message::PartialMessage;
use compact_str::CompactString;
use std::error::Error;
use std::fmt;
use std::str::Utf8Error;

pub enum CoreControlMessage {
    CHDRV(ChangeDriverMessage),
    UNKNOWN(String),
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
}

impl Error for MessageParseErrorKind {}

impl fmt::Display for MessageParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            MessageParseErrorKind::MissingKey => write!(f, "Message key is missing"),
            MessageParseErrorKind::MissingPayload => write!(f, "Message payload is missing"),
            MessageParseErrorKind::InvalidPayload(decoding_error) => {
                write!(f, "Failed to decode payload: {decoding_error}")
            }
        }
    }
}

impl TryFrom<PartialMessage> for CoreControlMessage {
    type Error = MessageParseError;

    fn try_from(message: PartialMessage) -> Result<Self, Self::Error> {
        let key_bytes = match &message.key {
            Some(key) => key,
            None => {
                return Err(MessageParseError {
                    message,
                    kind: MessageParseErrorKind::MissingKey,
                });
            }
        };

        match key_bytes.as_ref() {
            b"CHDRV" => {
                let payload_bytes = match message.payload.as_deref() {
                    Some(p) => p,
                    None => {
                        return Err(MessageParseError {
                            message,
                            kind: MessageParseErrorKind::MissingPayload,
                        });
                    }
                };
                match CompactString::from_utf8(payload_bytes) {
                    Ok(driver_id) => Ok(CoreControlMessage::CHDRV(ChangeDriverMessage {
                        new_driver_id: driver_id,
                    })),
                    Err(decoding_error) => Err(MessageParseError {
                        message,
                        kind: MessageParseErrorKind::InvalidPayload(decoding_error),
                    }),
                }
            }
            unknown => Ok(CoreControlMessage::UNKNOWN(
                String::from_utf8_lossy(unknown).into_owned(),
            )),
        }
    }
}
