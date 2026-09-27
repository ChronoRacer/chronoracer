mod queue;
mod serve;

pub use queue::PreviewHub;
pub use serve::serve;

pub mod proto {
    #![allow(clippy::pedantic, clippy::nursery, clippy::cargo)]
    tonic::include_proto!("chronoracer.telemetry.v1");
}
