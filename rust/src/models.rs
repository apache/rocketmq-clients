use std::{
    collections::HashMap,
    sync::Arc,
    sync::{atomic::AtomicUsize, Weak},
};

pub(crate) struct MessageImpl {
    pub(crate) keys: Vec<String>,
    pub(crate) body: Vec<u8>,
    pub(crate) topic: String,
    pub(crate) tags: String,
    pub(crate) messageGroup: String,
    pub(crate) deliveryTimestamp: i64,
    pub(crate) properties: HashMap<String, String>,
}

impl MessageImpl {
    pub fn new(
        topic: &str,
        tags: &str,
        keys: Vec<String>,
        body: &str,
    ) -> Self {
        MessageImpl {
            keys: keys,
            body: body.as_bytes().to_vec(),
            topic: topic.to_string(),
            tags: tags.to_string(),
            messageGroup: "".to_string(),
            deliveryTimestamp: 0,
            properties: HashMap::new(),
        }
    }
}