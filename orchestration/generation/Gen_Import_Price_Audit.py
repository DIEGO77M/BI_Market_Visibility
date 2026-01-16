
"""
File: Gen_Import_Price_Audit.py
Description: Synthetic data generator for product master (raw) to simulate price audit scenarios in retail/FMCG analytics projects.
Author: BI Market Visibility Project

Purpose:
    - Generates a mock product master dataset with both own and competitor products.
    - Simulates realistic segmentation, pricing, and brand structure for price audit and analytics use cases.
    - Intended for use in environments where real raw data is unavailable or restricted.

Inputs:
    - No external input required. All data is generated randomly based on configuration parameters.

Outputs:
    - CSV file: product_master_raw.csv (in the script's execution directory)
    - Console printout with the shape of the generated DataFrame.

Usage:
    python Gen_Import_Price_Audit.py

Notes:
    - This script is for development, testing, and demo purposes only. Not for production use.
    - All business logic, segments, and price ranges are synthetic and do not represent real company data.
"""

import pandas as pd
import random
import numpy as np

random.seed(42)
np.random.seed(42)

############################################################
# CONFIGURATION
############################################################
NUM_OWN_PRODUCTS = 500
NUM_COMP_PRODUCTS = 600

SEGMENTS = [
    ("Culinary", "Seasoning and Condiments", "AMBIENT CULINARY", "CBR L4 Dehy Seasoning"),
    ("Culinary", "Sauces", "AMBIENT CULINARY", "CBR L4 Sauces"),
    ("Culinary", "Soups", "AMBIENT CULINARY", "CBR L4 Soups"),
    ("Dairy", "Powdered Milk", "DAIRY", "CBR L4 Powdered Milk"),
    ("Beverages", "Coffee", "BEVERAGES", "CBR L4 Coffee")
]

COMPETITIVE_GROUPS = {
    "CHICKEN_SEASONING": (25, 45),
    "BEEF_SEASONING": (30, 55),
    "TOMATO_SAUCE": (20, 40),
    "SOUP_CUBES": (10, 25),
    "INSTANT_COFFEE": (40, 90)
}

OWN_BRANDS = ["MAGGI", "NESCAFÉ", "KITANO"]
COMP_BRANDS = ["Knorr", "Local Brand", "Premium Select", "Generic Food"]

############################################################
# GENERATOR
############################################################
def generate_products(start_code, n, brand_type):
    """
    Generate a list of synthetic products for the product master.
    Args:
        start_code (int): Starting integer for product codes.
        n (int): Number of products to generate.
        brand_type (str): 'Own' for own brands, 'Competitor' for competitors.
    Returns:
        list of dict: Each dict represents a product with attributes.
    """
    products = []
    codes = range(start_code, start_code + n)

    for code in codes:
        group = random.choice(list(COMPETITIVE_GROUPS.keys()))
        min_p, max_p = COMPETITIVE_GROUPS[group]
        base_price = round(random.uniform(min_p, max_p), 2)

        segment, subsegment, category, subcategory = random.choice(SEGMENTS)
        brand = random.choice(OWN_BRANDS if brand_type == "Own" else COMP_BRANDS)

        product_name = f"{brand} {group.replace('_', ' ').title()} {random.choice(['200g', '400g', '500g'])}"

        products.append({
            "Product_Code": f"{'P' if brand_type == 'Own' else 'C'}{code}",
            "Product_Name": product_name,
            "Reference_Price_USD": base_price,
            "Segment": segment,
            "Subsegment": subsegment,
            "Category": category,
            "Subcategory": subcategory,
            "Brand_Type": brand_type,
            "Competitive_Group": group
        })

    return products

############################################################
# BUILD MASTER DATASET
############################################################
own_products = generate_products(100000, NUM_OWN_PRODUCTS, "Own")
comp_products = generate_products(200000, NUM_COMP_PRODUCTS, "Competitor")

df_products = pd.DataFrame(own_products + comp_products)

############################################################
# EXPORT
############################################################
df_products.to_csv("product_master_raw.csv", index=False)

print("Product master generated:", df_products.shape)
