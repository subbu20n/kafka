import argparse

from kafka import KafkaConsumer

from kafka_zero_to_hero.common import DEFAULT_BOOTSTRAP_SERVERS, json_deserializer, unique_group_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously consume demo order events")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-events-live")
    parser.add_argument(
        "--group-id",
        default="",
        help="Optional. If empty, a new group id is generated for each run.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from earliest offset instead of only new events.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    group_id = args.group_id or unique_group_id("live-demo-consumer")
    offset_policy = "earliest" if args.from_beginning else "latest"

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=offset_policy,
        enable_auto_commit=True,
        value_deserializer=json_deserializer,
    )

    print(f"CONSUMER_TOPIC={args.topic}")
    print(f"CONSUMER_GROUP={group_id}")
    print(f"OFFSET_POLICY={offset_policy}")
    print("Consumer is running. Press Ctrl+C to stop.")

    try:
        for message in consumer:
            payload = message.value or {}
            print(
                "CONSUMED_EVENT "
                f"partition={message.partition} offset={message.offset} "
                f"order_id={payload.get('order_id')} status={payload.get('status')} at={payload.get('event_time')}"
            )
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())