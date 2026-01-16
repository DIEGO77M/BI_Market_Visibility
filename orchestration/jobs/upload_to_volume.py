"""
Script: upload_to_volume.py
Description: Simulates the automated upload of raw data from a cloud source (e.g., S3) to the Databricks Unity Catalog Volume workspace.raw_data.
This is a simulation for architecture demonstration purposes. No real files are transferred.
Author: Diego Mayorga
"""


import time


# Simulation configuration
PROFILE = "DTB_Market_Visibility"
VOLUME_PATH = "/Volumes/workspace/raw_data/"
CLOUD_SOURCE_PATHS = [
    "s3://company-bucket/raw/Master_PDV/",
    "s3://company-bucket/raw/Master_Products/",
    "s3://company-bucket/raw/raw_price_audit/",
    "s3://company-bucket/raw/sell_in_output/"
]


def simulate_upload(cloud_path: str, volume_path: str, profile: str) -> None:
    # Simulate the transfer delay
    folder_name = cloud_path.rstrip('/').split('/')[-1]
    target_path = f"{volume_path}{folder_name}"
    print(f"[SIMULATION] Starting upload from {cloud_path} to {target_path} using profile {profile}...")
    time.sleep(2)  # Simulate time delay
    print(f"[SIMULATION] Upload from {cloud_path} to {target_path} completed successfully.")


def main():
    print("[SIMULATION] Automated cloud-to-Databricks upload simulation started.")
    for cloud_path in CLOUD_SOURCE_PATHS:
        simulate_upload(cloud_path, VOLUME_PATH, PROFILE)
    print("[SIMULATION] All simulated uploads completed. No real files were transferred.")


if __name__ == "__main__":
    main()
