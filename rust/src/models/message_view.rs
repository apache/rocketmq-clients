#[derive(Debug)]
pub(crate) struct MessageView {
    pub(crate) body:Vec<u8>,
    pub(crate) message_id:String,
    pub(crate) topic:String,
    pub(crate) consume_group :String,
    pub(crate) endpoint :String,
    pub(crate) receipt_handle :String,
}