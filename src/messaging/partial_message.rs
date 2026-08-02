use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

#[derive(Debug)]
pub struct PartialMessage {
    pub key: Option<Box<[u8]>>,
    pub offset: i64,
    pub partition: i32,
    pub payload: Option<Box<[u8]>>,
}

impl From<BorrowedMessage<'_>> for PartialMessage {
    fn from(borrowed_message: BorrowedMessage) -> Self {
        Self {
            offset: borrowed_message.offset(),
            partition: borrowed_message.partition(),
            key: borrowed_message.key().map(|bytes| Box::from(bytes)),
            payload: borrowed_message.payload().map(|bytes| Box::from(bytes)),
        }
    }
}

pub struct PartialMessageWithError {
    pub message: PartialMessage,
    pub error_msg: String,
}

impl PartialMessageWithError {
    pub(crate) fn new<E: std::error::Error + 'static>(original: PartialMessage, err: &E) -> Self {
        Self {
            message: original,
            error_msg: err.to_string(),
        }
    }
}