use crate::messaging::dlq_handler::InvalidMessageTx;
use crate::messaging::message_sink::{BatchSinkRx, SinkRx};
use crate::messaging::offset_ledger::OffsetLedger;
use crate::messaging::partial_message::SourceOffset;
use crate::persistence::persistence_entrypoint::{SensorDataBatch, SensorDataRx};
use crate::processing::control_filter::ControlFilter;
use crate::processing::reading_collector::ReadingCollector;
use crate::runtime_configuration::RetainReadTimeValue;
use crate::telemetry::PreviewHub;
use anyhow::Result;
use compact_str::CompactString;
use std::sync::Arc;
use tokio::sync::oneshot;

pub struct ProcessingLayerEntrypoint {
    pub(crate) collector: ReadingCollector,
    pub(crate) processed_data_rx: SensorDataRx,
    pub(crate) control_filter: ControlFilter,
}

impl ProcessingLayerEntrypoint {
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::needless_pass_by_value)]
    pub fn init(
        sensor_sink_rx: BatchSinkRx,
        control_rx: SinkRx,
        dlq_tx: InvalidMessageTx,
        chdrv_handler: &'static RetainReadTimeValue<CompactString>,
        shutdown_handler_tx: oneshot::Sender<SourceOffset>,
        control_offsets: OffsetLedger,
        rayon_pool: Arc<rayon::ThreadPool>,
        preview: PreviewHub,
    ) -> Result<Self> {
        let (processed_data_tx, processed_data_rx) =
            tokio::sync::mpsc::channel::<SensorDataBatch>(rayon_pool.current_num_threads());

        let collector = ReadingCollector::new(
            sensor_sink_rx,
            dlq_tx.clone(),
            processed_data_tx,
            Arc::clone(&rayon_pool),
            preview,
        );
        let control_filter = ControlFilter::new(
            control_rx,
            dlq_tx,
            chdrv_handler,
            shutdown_handler_tx,
            control_offsets,
        );
        Ok(Self {
            collector,
            processed_data_rx,
            control_filter,
        })
    }
}
