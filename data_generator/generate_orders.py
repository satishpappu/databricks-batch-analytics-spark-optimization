import argparse
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from src.utils.config_loader import load_config


fake = Faker()

ORDER_STATUS = [
    "pending",
    "processing",
    "completed",
    "canceled",
    "rejected",
]

PAYMENT_METHODS = [
    "debit",
    "credit",
]


def generate_orders(
    output_path: str,
    num_orders: int,
    num_customers: int,
    num_stores: int,
    max_items_per_order: int,
) -> None:
    rows = []

    for order_id in range(1, num_orders + 1):
        rows.append(
            {
                "order_id": order_id,
                "customer_id": random.randint(1, num_customers),
                "order_timestamp": fake.date_time_between(
                    start_date="-5y",
                    end_date="now",
                ),
                "store_id": random.randint(1, num_stores),
                "order_status": random.choice(ORDER_STATUS),
                "payment_method": random.choice(PAYMENT_METHODS),
                "total_amount": round(random.uniform(5, 10000), 2),
                "item_count": random.randint(1, max_items_per_order),
            }
        )

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "orders.csv"

    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)

    print(f"Generated {num_orders} orders at {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    raw_data_path = config["paths"]["raw_data"]
    num_orders = config["data_generation"]["orders"]
    num_customers = config["data_generation"]["customers"]
    num_stores = config["data_generation"]["stores"]
    max_items_per_order = config["data_generation"]["max_items_per_order"]

    generate_orders(
        output_path=f"{raw_data_path}/orders",
        num_orders=num_orders,
        num_customers=num_customers,
        num_stores=num_stores,
        max_items_per_order=max_items_per_order,
    )