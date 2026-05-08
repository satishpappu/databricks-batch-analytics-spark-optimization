import argparse
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from src.utils.config_loader import load_config


fake = Faker()

CATEGORIES = [
    "electronics",
    "clothing",
    "home",
    "grocery",
    "beauty",
    "sports",
    "toys",
    "automotive",
]


def weighted_category(hot_category: str, hot_category_ratio: float) -> str:
    if random.random() < hot_category_ratio:
        return hot_category

    remaining_categories = [category for category in CATEGORIES if category != hot_category]
    return random.choice(remaining_categories)


def generate_products(
    output_path: str,
    num_products: int,
    hot_category: str,
    hot_category_ratio: float,
) -> None:
    rows = []

    for product_id in range(1, num_products + 1):
        category = weighted_category(hot_category, hot_category_ratio)

        rows.append(
            {
                "product_id": product_id,
                "product_name": f"{fake.word().title()} {fake.word().title()}",
                "category": category,
                "brand": fake.company(),
                "unit_price": round(random.uniform(5, 2000), 2),
                "is_active": random.choice([True, True, True, False]),
            }
        )

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "products.csv"

    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)

    print(f"Generated {num_products} products at {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    raw_data_path = config["paths"]["raw_data"]
    num_products = config["data_generation"]["products"]
    hot_category = config["skew"]["hot_category"]
    hot_category_ratio = config["skew"]["hot_category_ratio"]

    generate_products(
        output_path=f"{raw_data_path}/products",
        num_products=num_products,
        hot_category=hot_category,
        hot_category_ratio=hot_category_ratio,
    )