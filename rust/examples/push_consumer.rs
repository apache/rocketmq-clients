use rocketmq::{conf::{ClientOption, PushConsumerOption}, model::{common::{ConsumeResult, FilterExpression, FilterType}, message::MessageView}, MessageListener, PushConsumer};
use tokio::time;
use std::{collections::HashMap, time::Duration};

#[tokio::main]
async fn main() {
    let mut client_option = ClientOption::default();
    client_option.set_access_url("localhost:8081");
    client_option.set_enable_tls(false);
    let mut option = PushConsumerOption::default();
    option.set_consumer_group("test");
    let mut subscription_expressions: HashMap<String, FilterExpression> = HashMap::new();
    subscription_expressions.insert("test_topic".to_string(), FilterExpression::new(FilterType::Tag, "*"));
    option.set_subscription_expressions(subscription_expressions);
    
    let callback: Box<MessageListener> = Box::new(|message| {
        println!("Receive message: {:?}", message);
        ConsumeResult::SUCCESS
    });
    
    let mut push_consumer = PushConsumer::new(client_option, option, callback).unwrap();
    push_consumer.start().await;

    time::sleep(Duration::from_secs(60)).await;

    push_consumer.shutdown().await;
}