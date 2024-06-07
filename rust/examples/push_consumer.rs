use rocketmq::{
    conf::{ClientOption, PushConsumerOption},
    model::common::{ConsumeResult, FilterExpression, FilterType},
    MessageListener, PushConsumer,
};
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() {
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);

    let mut option = PushConsumerOption::default();
    option.set_consumer_group("test");
    option.subscribe("test_topic", FilterExpression::new(FilterType::Tag, "*"));

    let callback: Box<MessageListener> = Box::new(|message| {
        println!("Receive message: {:?}", message);
        ConsumeResult::SUCCESS
    });

    let mut push_consumer = PushConsumer::new(client_option, option, callback).unwrap();
    let start_result = push_consumer.start().await;
    if start_result.is_err() {
        eprintln!("push consumer start failed: {:?}", start_result.unwrap_err());
        return;
    }

    time::sleep(Duration::from_secs(60)).await;

    let _ = push_consumer.shutdown().await;
}
