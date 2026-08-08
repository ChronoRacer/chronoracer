use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::SinkRx;
use crate::persistence::persistence_entrypoint::SensorDataRx;
use crate::processing::avro::{AvroSchema, SchemaRegistry};
use crate::processing::control_filter::ControlFilter;
use crate::processing::reading_collector::ReadingCollector;
use crate::processing::sensor_data_message::UnwoundSensorDataMessage;
use crate::runtime_configuration::RetainReadTimeValue;
use compact_str::CompactString;
use std::sync::Arc;

pub struct ProcessingLayerEntrypoint {
    pub(crate) collector: ReadingCollector,
    pub(crate) processed_data_rx: SensorDataRx,
    pub(crate) control_filter: ControlFilter,
}

impl ProcessingLayerEntrypoint {

    #[must_use]
    pub fn init(
        sensor_sink_rx: SinkRx,
        control_rx: SinkRx,
        dlq_tx: InvalidMessageTx,
        chdrv_handler: &'static RetainReadTimeValue<CompactString>,
        rayon_pool: Arc<rayon::ThreadPool>,
    ) -> Self {
        let (processed_data_tx, processed_data_rx) = tokio::sync::mpsc::channel::<
            UnwoundSensorDataMessage,
        >(rayon_pool.current_num_threads());

        let sensor_data_schema = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData)
            .expect("Failed to load sensor data schema");
        let dlq_tx_arc = Arc::from(dlq_tx);
        let collector = ReadingCollector::new(
            sensor_sink_rx,
            Arc::clone(&dlq_tx_arc),
            processed_data_tx,
            sensor_data_schema,
            Arc::clone(&rayon_pool),
        );
        let control_filter = ControlFilter::new(
            control_rx,
            Arc::clone(&dlq_tx_arc),
            chdrv_handler,
            Arc::clone(&rayon_pool),
        );
        Self {
            collector,
            processed_data_rx,
            control_filter,
        }
    }
}
