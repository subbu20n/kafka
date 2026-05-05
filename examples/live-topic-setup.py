import argparse

from kafka_zero_to_hero.common import (
    DEFAULT_BOOTSTRAP_SERVERS,
    ensure_topics,
    wait_for_broker,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create topic for the live producer/consumer demo")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-events-live")
    parser.add_argument("--partitions", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    wait_for_broker(args.bootstrap_servers)
    ensure_topics(
        [(args.topic, args.partitions, 1)],
        bootstrap_servers=args.bootstrap_servers,
    )

    print(f"TOPIC_READY={args.topic}")
    print(f"BOOTSTRAP_SERVERS={args.bootstrap_servers}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())