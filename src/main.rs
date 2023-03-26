use std::time::Duration;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use coinbase_pro_rs::{structs::wsfeed::{ChannelType, Message, Full, Done, Open}, WSFeed, WS_URL};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;


#[derive(Serialize)]
enum CoinbaseMessage {
    Done(Done),
    Open(Open)
}

fn produce(topic_name: &str, coinbaseMessage: CoinbaseMessage) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", "localhost:29092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let id = match coinbaseMessage {
        CoinbaseMessage::Done(Done::Limit { order_id, .. }) => order_id,
        CoinbaseMessage::Done(Done::Market { order_id, .. }) => order_id,
        CoinbaseMessage::Open(ref open) => open.order_id,
    };

    let msg = match coinbaseMessage {
        CoinbaseMessage::Done(Done::Limit { product_id, .. } ) => { product_id },
        CoinbaseMessage::Done(Done::Market { product_id, .. }) => { product_id },
        CoinbaseMessage::Open(Open { product_id, .. } ) => { product_id },
    };

    println!("Sending message {:?}", &serde_json::to_string(&msg).unwrap());
    producer
        .send(
            FutureRecord::to(topic_name)
                .payload(&serde_json::to_string(&msg).unwrap())
                .key(&format!("{:?}", id)),
            Duration::from_secs(0),
        );
}

#[tokio::main]
async fn main() {
    let stream = WSFeed::connect(WS_URL, &["BTC-USD"], &[ChannelType::Full])
        .await
        .unwrap();



    stream
        .take(10000)
        .try_for_each(|message| async {
            match message {
                Message::Full(Full::Done(msg)) => produce("coinbase_btc_usd_done", CoinbaseMessage::Done(msg)),
                Message::Full(Full::Open(msg)) => produce("coinbase_btc_usd_open", CoinbaseMessage::Open(msg)),
                Message::Error { message } => println!("Error: {}", message),
                Message::InternalError(_) => (),
                _ => (),
            }
            Ok(())
        })
        .await
        .expect("stream fail");
}