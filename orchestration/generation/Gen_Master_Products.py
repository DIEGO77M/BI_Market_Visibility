
"""
File: Gen_Master_Products.py
Description: Synthetic product master generator using price audit files for retail/FMCG analytics projects.
Author: BI Market Visibility Project

Purpose:
    - Builds a mock product master dataset for own products based on detected price audit files.
    - Simulates segmentation, brand assignment, and product structure for analytics and demo use cases.
    - Intended for use in environments where real product master data is unavailable or restricted.

Inputs:
    - Excel files matching pattern: D:/Projects/Inventory_Nestle/raw_price_audit/Price_Audit_*.xlsx
      (Each file must contain columns: Cod_Producto, Nombre_Producto)

Outputs:
    - CSV file: product_master_raw.csv (in the script's execution directory)
    - Console printout with summary statistics of the generated DataFrame.

Usage:
    python Gen_Master_Products.py

Notes:
    - This script is for development, testing, and demo purposes only. Not for production use.
    - All business logic, segments, and brands are synthetic and do not represent real company data.
"""

import pandas as pd
import glob
import random

random.seed(42)

############################################################
# CONFIGURATION
############################################################
PRICE_AUDIT_PATH = r"D:\Projects\Inventory_Nestle\raw_price_audit\Price_Audit_*.xlsx"
OUTPUT_FILE = "product_master_raw.csv"

SEGMENT_MAP = [
    ("Culinary", "Seasoning and Condiments", "AMBIENT CULINARY", "CBR L4 Dehy Seasoning"),
    ("Culinary", "Sauces", "AMBIENT CULINARY", "CBR L4 Sauces"),
    ("Culinary", "Soups", "AMBIENT CULINARY", "CBR L4 Soups"),
    ("Dairy", "Powdered Milk", "DAIRY", "CBR L4 Powdered Milk"),
    ("Beverages", "Coffee", "BEVERAGES", "CBR L4 Coffee")
]

BRANDS = ["MAGGI", "NESCAFÉ", "KITANO"]

############################################################
# LOAD PRICE AUDIT FILES
############################################################
files = glob.glob(PRICE_AUDIT_PATH)

if not files:
    raise FileNotFoundError(
        f"No Price Audit files found in path: {PRICE_AUDIT_PATH}"
    )

print(f"Price Audit files detected: {len(files)}")

df_list = [pd.read_excel(f) for f in files]
price_audit = pd.concat(df_list, ignore_index=True)

############################################################
# FILTER OWN PRODUCTS
############################################################
own_products = (
    price_audit[
        price_audit["Cod_Producto"].astype(str).str.startswith("P")
    ][["Cod_Producto", "Nombre_Producto"]]
    .drop_duplicates()
)

############################################################
# BUILD PRODUCT MASTER
############################################################
master_rows = []

for _, row in own_products.iterrows():
    segment, subsegment, category, subcategory = random.choice(SEGMENT_MAP)
    brand = next((b for b in BRANDS if b in row["Nombre_Producto"]), random.choice(BRANDS))

    master_rows.append({
        "Product_Code": row["Cod_Producto"],
        "Product_Name": row["Nombre_Producto"],
        "Brand": brand,
        "Segment": segment,
        "Subsegment": subsegment,
        "Category": category,
        "Subcategory": subcategory
    })

product_master = pd.DataFrame(master_rows)

############################################################
# EXPORT
############################################################
product_master.to_csv(OUTPUT_FILE, index=False)

print("Product Master generated successfully")
print("Total own products:", product_master.shape[0])
