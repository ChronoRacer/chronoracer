use crate::messaging::sensor_data_message::UnwoundSensorDataMessage;

pub(crate) struct ReadingCollector {
    
}

impl ReadingCollector {
    pub(crate) fn new() -> Self {
        Self {}
    }
    pub fn accept(mut self: &Self, data: UnwoundSensorDataMessage) {
        // TODO
    }
}