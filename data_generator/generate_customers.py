import argparse
import random
from pathlib import Path

import pandas as pd
from faker import Faker

from src.utils.config_loader import load_config


fake = Faker()

PROVINCES = [
    "Ontario",
    "Quebec",
    "British Columbia",
    "Alberta",
    "Manitoba",
]

PROVINCE_CITIES = {
    "Ontario": ["Toronto", "Ottawa", "Mississauga", "Brampton", "Hamilton"],
    "Quebec": ["Montreal", "Quebec City", "Laval", "Gatineau"],
    "British Columbia": ["Vancouver", "Victoria", "Surrey", "Burnaby"],
    "Alberta": ["Calgary", "Edmonton", "Red Deer"],
    "Manitoba": ["Winnipeg", "Brandon"],
}

LOYALTY_TIERS = ["bronze", "silver", "gold"]


def weighted_region(hot_region: str, hot_region_ratio: float) -> str:
    if random.random() < hot_region_ratio:
        return hot_region

    remaining_provinces = [
        province for province in PROVINCES if province != hot_region
    ]
    return random.choice(remaining_provinces)


def generate_customers(
    output_path: str,
    num_customers: int,
    hot_region: str,
    hot_region_ratio: float,
) -> None:
    rows = []

    for customer_id in range(1, num_customers + 1):
        province = weighted_region(hot_region, hot_region_ratio)
        city = random.choice(PROVINCE_CITIES[province])

        rows.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "city": city,
                "province": province,
                "signup_date": fake.date_between(start_date="-5y", end_date="today"),
                "loyalty_tier": random.choice(LOYALTY_TIERS),
                "is_active": random.choice([True, True, True, False]),
            }
        )

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "customers.csv"

    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)

    print(f"Generated {num_customers} customers at {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    raw_data_path = config["paths"]["raw_data"]
    num_customers = config["data_generation"]["customers"]
    hot_region = config["skew"]["hot_region"]
    hot_region_ratio = config["skew"]["hot_region_ratio"]

    generate_customers(
        output_path=f"{raw_data_path}/customers",
        num_customers=num_customers,
        hot_region=hot_region,
        hot_region_ratio=hot_region_ratio,
    )