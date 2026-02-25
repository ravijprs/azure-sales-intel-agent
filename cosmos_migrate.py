"""
Cosmos DB Migration Script
- Deletes all existing docs in maingraph container
- Upserts cosmos_salesmen.json (100 docs) and cosmos_regions.json (10 docs)

Usage:
  pip install azure-cosmos azure-identity
  python cosmos_migrate.py

Auth: Uses DefaultAzureCredential (Azure CLI login recommended: `az login`)
Or set env var COSMOS_KEY for key-based auth.
"""

import json
import os
from azure.cosmos import CosmosClient, PartitionKey
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
load_dotenv(override=True)

ACCOUNT = "salesgraph"
DATABASE = "salesIntel"
CONTAINER = "maingraph"
ENDPOINT = f"https://salesgraph.documents.azure.com:443/"

# --- Auth ---
key = os.getenv("COSMOS_KEY")
if key:
    client = CosmosClient(ENDPOINT, credential=key)
else:
    client = CosmosClient(ENDPOINT, credential=DefaultAzureCredential())

db = client.get_database_client(DATABASE)
container = db.get_container_client(CONTAINER)

# --- Step 1: Delete all existing documents ---
print("Fetching all existing documents...")
items = list(container.query_items("SELECT c.id, c.pk FROM c", enable_cross_partition_query=True))
print(f"Found {len(items)} docs to delete.")

for item in items:
    container.delete_item(item=item["id"], partition_key=item["pk"])
    print(f"  Deleted: {item['id']}")

print(f"✅ Deleted {len(items)} documents.\n")

# --- Step 2: Upsert new documents ---
files = ["cosmos_salesmen.json", "cosmos_regions.json"]

for fname in files:
    with open(fname, "r") as f:
        docs = json.load(f)
    print(f"Upserting {len(docs)} docs from {fname}...")
    for doc in docs:
        container.upsert_item(doc)
    print(f"  ✅ Done: {fname}\n")

print("🎉 Migration complete!")