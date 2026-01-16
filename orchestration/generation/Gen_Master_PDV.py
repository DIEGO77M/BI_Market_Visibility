
"""
File: Gen_Master_PDV.py
Description: Synthetic data generator for master PDV (point of sale) to simulate store universe for retail/FMCG analytics projects.
Author: BI Market Visibility Project

Purpose:
    - Generates a mock master PDV dataset with realistic store, channel, and personnel attributes.
    - Simulates segmentation, geolocation, and commercial structure for analytics and demo use cases.
    - Intended for use in environments where real raw data is unavailable or restricted.

Inputs:
    - No external input required. All data is generated randomly based on configuration parameters.

Outputs:
    - CSV file: master_pdv_raw.csv (in the script's execution directory)
    - Console printout with summary statistics of the generated DataFrame.

Usage:
    python Gen_Master_PDV.py

Notes:
    - This script is for development, testing, and demo purposes only. Not for production use.
    - All business logic, names, and geolocations are synthetic and do not represent real company data.
"""

import pandas as pd
import random
import numpy as np

random.seed(42)
np.random.seed(42)

############################################################
# CONFIGURATION
############################################################
NUM_STORES = 50
OUTPUT_FILE = "master_pdv_raw.csv"

CHANNELS = [
    ("Direct Trade", "Independent Supermarket"),
    ("Direct Trade", "Convenience Store"),
    ("Modern Trade", "Supermarket Chain"),
    ("Modern Trade", "Hypermarket")
]

CITIES = [
    ("Brown's Town", "St Ann"),
    ("Kingston", "Kingston"),
    ("Montego Bay", "St James"),
    ("Mandeville", "Manchester"),
    ("Ocho Rios", "St Ann")
]

SUPERVISORS = [
    ("SUP-001", "Patrick James"),
    ("SUP-002", "Andrew Collins"),
    ("SUP-003", "Michael Brown"),
    ("SUP-004", "Kevin Thompson"),
    ("SUP-005", "Daniel Lewis")
]

# 🔹 20 MERCHANDISERS
MERCHANDISERS = [
    ("MER-001", "Orville Tennyson"),
    ("MER-002", "Shawn Williams"),
    ("MER-003", "Andre Miller"),
    ("MER-004", "Jason Reid"),
    ("MER-005", "Kemar Johnson"),
    ("MER-006", "Dwayne Smith"),
    ("MER-007", "Ricardo Grant"),
    ("MER-008", "Leroy Campbell"),
    ("MER-009", "Alton Brooks"),
    ("MER-010", "Mark Bennett"),
    ("MER-011", "Clive Morgan"),
    ("MER-012", "Anthony Foster"),
    ("MER-013", "Devon Clarke"),
    ("MER-014", "Paul Edwards"),
    ("MER-015", "Ryan Scott"),
    ("MER-016", "Leon Mitchell"),
    ("MER-017", "Carl Bennett"),
    ("MER-018", "Marvin Lewis"),
    ("MER-019", "Desmond Grant"),
    ("MER-020", "Trevor Hall")
]

SALES_REPS = [
    "Shakeera Marshall",
    "Nicole Adams",
    "Tanya Brown",
    "Kevin Wright",
    "Omar Johnson",
    "Samuel Green"
]

############################################################
# GENERATE STORES
############################################################
rows = []

for i in range(1, NUM_STORES + 1):
    store_id = f"{i:03d}"

    channel, sub_channel = random.choice(CHANNELS)
    city, parish = random.choice(CITIES)
    supervisor_code, supervisor_name = random.choice(SUPERVISORS)
    merch_code, merch_name = random.choice(MERCHANDISERS)

    rows.append({
        "Code (eLeader)": f"PDV{store_id}",
        "Store Name": f"Store_{store_id}",
        "Channel": channel,
        "Sub Channel": sub_channel,
        "Chain": "Independent" if "Independent" in sub_channel else "Chain",
        "Neighborhood": city,
        "City": city,
        "Parish": parish,
        "Country": "Jamaica",
        "Latitude": round(random.uniform(17.9, 18.6), 7),
        "Longitude": round(random.uniform(-78.5, -76.2), 7),
        "Type of Service": "MERCHANDISER",
        # 🔹 100% ACTIVE
        "Status": "ACTIVE",
        "Supervisor Code": supervisor_code,
        "Supervisor Name": supervisor_name,
        "Merchandiser Code": merch_code,
        "Merchandiser Name": merch_name,
        "CODE PO": f"PO_{store_id}",
        "Aditional_Exhibitions": random.choice(["Yes", "No"]),
        "Commercial Activities": random.choice(["Yes", "No"]),
        "Planograms": random.choice(["Yes", "No"]),
        "Store SAP Code": random.randint(3000000, 3999999),
        "Sales Rep": random.choice(SALES_REPS)
    })

master_pdv = pd.DataFrame(rows)

############################################################
# EXPORT
############################################################
master_pdv.to_csv(OUTPUT_FILE, index=False)

print("Master PDV generated successfully")
print("Total stores:", master_pdv.shape[0])
print("Unique merchandisers:", master_pdv["Merchandiser Code"].nunique())
print("Status distribution:")
print(master_pdv["Status"].value_counts())
