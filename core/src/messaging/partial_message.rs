use rdkafka::Message;
use rdkafka::message::BorrowedMessage;

/// Which consumer a message came from. Each has its own consumer group and offset ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageOrigin {
    #[default]
    SensorData,
    Control,
}

#[derive(Debug)]
pub struct PartialMessage {
    pub key: Option<Box<[u8]>>,
    pub offset: i64,
    pub partition: i32,
    pub payload: Option<Box<[u8]>>,
    pub origin: MessageOrigin,
}

/// Position of a consumed record in its source topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SourceOffset {
    pub origin: MessageOrigin,
    pub partition: i32,
    pub offset: i64,
}

impl PartialMessage {
    #[must_use]
    pub fn source_offset(&self) -> SourceOffset {
        SourceOffset {
            origin: self.origin,
            partition: self.partition,
            offset: self.offset,
        }
    }

    #[must_use]
    pub fn from_borrowed(borrowed_message: &BorrowedMessage<'_>, origin: MessageOrigin) -> Self {
        Self {
            offset: borrowed_message.offset(),
            partition: borrowed_message.partition(),
            key: borrowed_message.key().map(Box::from),
            payload: borrowed_message.payload().map(Box::from),
            origin,
        }
    }
}

pub struct PartialMessageWithError {
    pub message: PartialMessage,
    pub error_msg: String,
}

impl PartialMessageWithError {
    pub(crate) fn new<E: std::fmt::Display>(original: PartialMessage, err: &E) -> Self {
        Self {
            message: original,
            error_msg: err.to_string(),
        }
    }
}
