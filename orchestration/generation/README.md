# Data Generation Scripts – Executive Overview

## Executive Summary
This folder contains synthetic data generators used to simulate a real retail/FMCG analytics environment, inspired by actual Nestlé projects. All scripts are designed for demo, development, and portfolio purposes—no real company data is used.

**Business Impact**
- Enables realistic end-to-end analytics demos without exposing confidential data
- Demonstrates ability to model complex business scenarios (pricing, inventory, segmentation)
- Validates architecture and pipeline logic in a controlled, enterprise-like setting

---

## Why Synthetic Data?
- Real raw data is restricted due to confidentiality and compliance
- Synthetic datasets allow for safe, reproducible, and scalable demonstrations
- All business logic, segmentation, and metrics are inspired by real-world retail, but do not represent actual company data

---

## Script Overview

### Gen_Import_Price_Audit.py
- **Purpose**: Generates mock product master data for price audit scenarios (own vs competitor products)
- **Outputs**: product_master_raw.csv
- **Business Simulation**: Segmentation, pricing, and brand structure for competitive analytics

### Gen_Master_PDV.py
- **Purpose**: Creates synthetic master PDV (point of sale) data with realistic store, channel, and personnel attributes
- **Outputs**: master_pdv_raw.csv
- **Business Simulation**: Store segmentation, geolocation, and commercial hierarchy

### Gen_Master_Products.py
- **Purpose**: Builds mock product master dataset from simulated price audit files
- **Inputs**: Synthetic Excel files (simulating real audit sources)
- **Outputs**: product_master_raw.csv
- **Business Simulation**: Product segmentation, brand assignment, and structure

### Gen_Sell_In.py
- **Purpose**: Generates synthetic sell-in data with inventory, sales, and risk metrics for each product and PDV
- **Inputs**: master_pdv_raw.csv, product_master_raw.csv
- **Outputs**: Sell_In_2021.xlsx, Sell_In_2022.xlsx
- **Business Simulation**: Monthly inventory flows, replenishment, and stock risk

---

## Usage

- All scripts are run independently via Python
- Outputs are used as raw data sources for the Bronze layer in the analytics pipeline
- Designed for rapid prototyping, technical interviews, and executive demos

---

## Principle Highlights

| Principle              | Why It Matters                |
| ---------------------- | ----------------------------- |
| **Scannable**          | Recruiters skim, not read     |
| **Business-first**     | Value before technology       |
| **Architecture-aware** | Shows seniority               |
| **Decision-driven**    | Demonstrates judgment         |
| **Always current**     | Outdated data = low ownership |

---

> These generators simulate a real enterprise analytics environment, enabling business storytelling and technical validation without risk. All logic is defendable in senior interviews and executive reviews.
