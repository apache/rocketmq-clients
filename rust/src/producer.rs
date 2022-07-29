use slog::Logger;

use crate::{client, error};

struct Producer {
    client: client::Client,
}

impl Producer {
    pub async fn new<T>(logger: Logger, topics: T) -> Result<Self, error::ClientError>
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let access_point = "localhost:8081";
        let client = client::Client::new(logger, access_point)?;
        for _topic in topics.into_iter() {
            // client.subscribe(topic.as_ref()).await;
        }

        Ok(Producer { client })
    }

    pub fn start(&mut self) {}
}
