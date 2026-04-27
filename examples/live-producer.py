import argparse
import time
from datetime import datetime, timezone

from kafka_zero_to_hero.common import DEFAULT_BOOTSTRAP_SERVERS, build_producer


STATUSES = [
    "created",
    "accepted",
    "preparing",
    "ready_for_pickup",
    "out_for_delivery",
    "delivered",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously produce demo order events")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-events-live")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    parser.add_argument("--max-events", type=int, default=0, help="0 means run until Ctrl+C")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    producer = build_producer(bootstrap_servers=args.bootstrap_servers)

    print(f"PRODUCER_TOPIC={args.topic}")
    print("Producer is running. Press Ctrl+C to stop.")

    event_count = 0
    order_number = 1

    try:
        while True:
            order_id = f"live-order-{order_number:04d}"

            for step, status in enumerate(STATUSES, start=1):
                event_count += 1
                event = {
                    "order_id": order_id,
                    "status": status,
                    "status_step": step,
                    "restaurant": "spice-house",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                }
                producer.send(args.topic, key=event["order_id"].encode("utf-8"), value=event)
                producer.flush()

                print(
                    "PRODUCED_EVENT "
                    f"seq={event_count} order_id={event['order_id']} "
                    f"status={event['status']} step={event['status_step']} at={event['event_time']}"
                )

                if args.max_events > 0 and event_count >= args.max_events:
                    return 0

                time.sleep(args.interval)

            order_number += 1
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())   