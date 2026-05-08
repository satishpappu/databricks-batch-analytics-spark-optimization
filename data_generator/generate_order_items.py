import argparse
import random
from pathlib import Path

import pandas as pd

from src.utils.config_loader import load_config


def generate_order_items(
    output_path: str,
    orders_path: str,
    products_path: str,
) -> None:
    orders_df = pd.read_csv(orders_path)
    products_df = pd.read_csv(products_path)

    product_price_map = dict(
        zip(products_df["product_id"], products_df["unit_price"])
    )

    product_ids = products_df["product_id"].tolist()

    rows = []
    order_item_id = 1

    for _, order in orders_df.iterrows():
        order_id = int(order["order_id"])
        item_count = int(order["item_count"])

        selected_products = random.choices(product_ids, k=item_count)

        for product_id in selected_products:
            quantity = random.randint(1, 5)
            unit_price = float(product_price_map[product_id])
            line_total = round(quantity * unit_price, 2)

            rows.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": int(product_id),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

            order_item_id += 1

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "order_items.csv"

    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)

    print(f"Generated {len(rows)} order items at {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    raw_data_path = config["paths"]["raw_data"]

    orders_path = f"{raw_data_path}/orders/orders.csv"
    products_path = f"{raw_data_path}/products/products.csv"

    generate_order_items(
        output_path=f"{raw_data_path}/order_items",
        orders_path=orders_path,
        products_path=products_path,
    )