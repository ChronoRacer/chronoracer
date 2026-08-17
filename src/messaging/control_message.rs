use crate::messaging::partial_message::PartialMessage;
use compact_str::CompactString;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use std::str::Utf8Error;

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
                    Ok(driver_id) => Ok(CoreControlMessage::CHDRV(ChangeDriverMessage {
                        new_driver_id: driver_id,
                    })),
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
