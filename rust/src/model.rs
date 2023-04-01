use std::sync::Arc;
use tokio::sync::oneshot;
use crate::error::ClientError;
use crate::pb::MessageQueue;

#[derive(Debug)]
pub struct Route {
    pub queue: Vec<MessageQueue>,
}

#[derive(Debug)]
pub(crate) enum RouteStatus {
    Querying(Vec<oneshot::Sender<Result<Arc<Route>, ClientError>>>),
    Found(Arc<Route>),
}