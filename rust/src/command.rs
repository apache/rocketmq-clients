use crate::client::Client;
use crate::error::ClientError;
use crate::pb::{QueryRouteRequest, QueryRouteResponse};
use tokio::sync::oneshot;
use tonic::{Request, Response};

pub(crate) enum Command {
    QueryRoute {
        peer: String,
        request: Request<QueryRouteRequest>,
        tx: oneshot::Sender<Result<Response<QueryRouteResponse>, ClientError>>,
    },
}
