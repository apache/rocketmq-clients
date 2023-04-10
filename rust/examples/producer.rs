use rocketmq::{ClientOption, MessageImpl, Producer, ProducerOption};

#[tokio::main]
async fn main() {
    // specify which topic(s) you would like to send message to
    // producer will prefetch topic route when start and failed fast if topic not exist
    let mut producer_option = ProducerOption::default();
    producer_option.set_topics(vec!["test".to_string()]);

    // set which rocketmq proxy to connect
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081".to_string());

    // build and start producer
    let producer = Producer::new(producer_option, client_option).await.unwrap();
    producer.start().await.unwrap();

    // build message
    let message = MessageImpl::builder()
        .set_topic("test".to_string())
        .set_tags("mq_test_tag".to_string())
        .set_body("hello world".as_bytes().to_vec())
        .build()
        .unwrap();

    // send message to rocketmq proxy
    let result = producer.send_one(message).await;
    debug_assert!(result.is_ok(), "send message failed: {:?}", result);
    println!(
        "send message success, message_id={}",
        result.unwrap().message_id
    );
}
