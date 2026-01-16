# Upload local raw files to Databricks Volume (Unity Catalog)
#
# This guide describes how to upload files from specific local directories to the workspace.raw_data Volume in Databricks (Unity Catalog).
#
# Local source directories:
#   1. D:/Projects/Inventory_Nestle/Master_PDV
#   2. D:/Projects/Inventory_Nestle/Master_Products
#   3. D:/Projects/Inventory_Nestle/raw_price_audit
#   4. D:/Projects/Inventory_Nestle/data/sell_in_output
#
# Target Volume path:
#   /Volumes/workspace/raw_data/
#
# Recommended CLI commands (one per folder):
#
# databricks fs cp --profile DTB_Market_Visibility --recursive "D:/Projects/Inventory_Nestle/Master_PDV" /Volumes/workspace/raw_data/Master_PDV
# databricks fs cp --profile DTB_Market_Visibility --recursive "D:/Projects/Inventory_Nestle/Master_Products" /Volumes/workspace/raw_data/Master_Products
# databricks fs cp --profile DTB_Market_Visibility --recursive "D:/Projects/Inventory_Nestle/raw_price_audit" /Volumes/workspace/raw_data/raw_price_audit
# databricks fs cp --profile DTB_Market_Visibility --recursive "D:/Projects/Inventory_Nestle/data/sell_in_output" /Volumes/workspace/raw_data/sell_in_output
#
# You can automate this process in a batch script or Databricks job if required.
