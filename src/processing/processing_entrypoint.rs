use crate::messaging::partial_message::{PartialMessage, PartialMessageWithError};
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::reading_collector::ReadingCollector;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;

pub fn setup_processing_pipe(
    sink_rx: tokio::sync::mpsc::Receiver<PartialMessage>,
    dlq_tx: tokio::sync::mpsc::Sender<PartialMessageWithError>,
) -> (ReadingCollector, SensorDataRx) {

    let (processed_data_tx, processed_data_rx) =
        tokio::sync::mpsc::channel::<UnwoundSensorDataMessage>(
            10, // TODO size should be equal to rayon processing pool
        );
    let collector = ReadingCollector::new(sink_rx, dlq_tx, processed_data_tx);
    (collector, processed_data_rx)
}
