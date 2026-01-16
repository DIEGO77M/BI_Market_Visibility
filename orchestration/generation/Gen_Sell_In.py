
"""
File: Gen_Sell_In.py
Description: Synthetic sell-in data generator for retail/FMCG analytics projects.
Author: BI Market Visibility Project

Purpose:
    - Generates a mock sell-in dataset with realistic inventory, sales, and risk metrics for each product and point of sale (PDV).
    - Simulates monthly inventory flows, replenishment, and stock risk for analytics and demo use cases.
    - Intended for use in environments where real sell-in data is unavailable or restricted.

Inputs:
    - master_pdv_raw.csv: Master PDV file (must contain column 'Code (eLeader)')
    - product_master_raw.csv: Product master file (must contain column 'Product_Code')

Outputs:
    - Excel files: Sell_In_2021.xlsx, Sell_In_2022.xlsx (in data/sell_in_output/)
    - Console printout with summary statistics of the generated DataFrame.

Usage:
    python Gen_Sell_In.py

Notes:
    - This script is for development, testing, and demo purposes only. Not for production use.
    - All business logic, inventory flows, and risk levels are synthetic and do not represent real company data.
"""

import pandas as pd
import numpy as np
import random
from pathlib import Path

############################################################
# CONFIGURATION
############################################################
BASE_PATH = Path("data")
OUTPUT_PATH = BASE_PATH / "sell_in_output"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

MASTER_PDV_PATH = BASE_PATH / "master_pdv_raw.csv"
MASTER_PRODUCTS_PATH = BASE_PATH / "product_master_raw.csv"

YEARS = [2021, 2022]
MONTHS = range(1, 13)

np.random.seed(42)
random.seed(42)

############################################################
# LOAD MASTER PDV
############################################################
pdv_df = pd.read_csv(MASTER_PDV_PATH, sep=";")

PDV_CODE_COL = "Code (eLeader)"
if PDV_CODE_COL not in pdv_df.columns:
    raise ValueError(
        f"No se encontró '{PDV_CODE_COL}'. "
        f"Columnas disponibles: {pdv_df.columns.tolist()}"
    )

pdv_codes = pdv_df[PDV_CODE_COL].dropna().unique()

############################################################
# LOAD MASTER PRODUCTS (ROBUST)
############################################################
products_df = pd.read_csv(MASTER_PRODUCTS_PATH)

# Caso: todo vino en una sola columna
if len(products_df.columns) == 1:
    products_df = products_df.iloc[:, 0].str.split(",", expand=True)
    products_df.columns = [
        "Product_Code",
        "Product_Name",
        "Brand",
        "Segment",
        "Subsegment",
        "Category",
        "Subcategory"
    ]

REQUIRED_COL = "Product_Code"
if REQUIRED_COL not in products_df.columns:
    raise ValueError(
        f"No se encontró '{REQUIRED_COL}'. "
        f"Columnas disponibles: {products_df.columns.tolist()}"
    )

product_codes = products_df[REQUIRED_COL].dropna().unique()

############################################################
# BUSINESS LOGIC FUNCTIONS
############################################################
def opening_stock(year):
    return random.randint(80, 200) if year == 2021 else random.randint(60, 140)

def sell_in_units(year):
    return random.randint(40, 120) if year == 2021 else random.randint(70, 150)

def returns_units(year, sell_in):
    ratio = random.uniform(0.05, 0.12) if year == 2021 else random.uniform(0.01, 0.04)
    return int(sell_in * ratio)

def estimated_consumption(year, sell_in):
    factor = random.uniform(0.55, 0.80) if year == 2021 else random.uniform(0.70, 0.90)
    return int(sell_in * factor)

def inventory_turnover(sell_in, avg_stock):
    return round(sell_in / avg_stock, 2) if avg_stock > 0 else 0

def days_of_inventory(closing_stock, consumption):
    return int((closing_stock / consumption) * 30) if consumption > 0 else 0

############################################################
# SELL-IN DATA GENERATION
############################################################
records = []

for year in YEARS:
    for month in MONTHS:
        for pdv in pdv_codes:
            for product in product_codes:

                open_stock = opening_stock(year)
                sell_in = sell_in_units(year)
                returns = returns_units(year, sell_in)
                consumption = estimated_consumption(year, sell_in)

                closing_stock = max(open_stock + sell_in - returns - consumption, 0)
                avg_stock = (open_stock + closing_stock) / 2

                doi = days_of_inventory(closing_stock, consumption)
                turnover = inventory_turnover(sell_in, avg_stock)

                replenishment = "YES" if closing_stock < 50 else "NO"

                if doi > 60:
                    risk = "OVERSTOCK"
                elif doi < 15:
                    risk = "STOCKOUT_RISK"
                else:
                    risk = "BALANCED"

                records.append({
                    "Year": year,
                    "Month": month,
                    "PDV_Code": pdv,
                    "Product_Code": product,
                    "Opening_Stock_Units": open_stock,
                    "Sell_In_Units": sell_in,
                    "Returns_Units": returns,
                    "Closing_Stock_Units": closing_stock,
                    "Days_of_Inventory": doi,
                    "Inventory_Turnover": turnover,
                    "Replenishment_Flag": replenishment,
                    "Stock_Risk_Level": risk
                })

sell_in_df = pd.DataFrame(records)

############################################################
# VALIDATION
############################################################
assert (sell_in_df["Closing_Stock_Units"] >= 0).all()
assert sell_in_df.isnull().sum().sum() == 0

############################################################
# EXPORT
############################################################
for year in YEARS:
    sell_in_df[sell_in_df["Year"] == year].to_excel(
        OUTPUT_PATH / f"Sell_In_{year}.xlsx",
        index=False
    )

print("====================================")
print("SELL-IN GENERADO CORRECTAMENTE")
print(f"PDVs: {len(pdv_codes)}")
print(f"Productos: {len(product_codes)}")
print(f"Registros totales: {sell_in_df.shape[0]}")
print("====================================")
