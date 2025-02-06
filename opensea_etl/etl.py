import os
import json
import time
from datetime import datetime
import requests
import pandas as pd
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from custom_orm.base import Database, Table, Column


class OpenSeaETL:
    def __init__(self, api_key: str, db: Database):
        self.api_key = api_key
        self.db = db
        self.base_url = "https://api.opensea.io/api/v2/collections"
        self.headers = {
            "accept": "application/json",
            "x-api-key": api_key
        }
        self.raw_data_path = "raw_data"
        self.rate_limit_calls = 0
        self.rate_limit_reset = time.time()
        self.RATE_LIMIT_MAX = 2
        self.RATE_LIMIT_WINDOW = 60
        os.makedirs(self.raw_data_path, exist_ok=True)

    def _create_collections_table(self) -> Table:
        columns = [
            Column("id", "SERIAL", primary_key=True),
            Column("collection", "VARCHAR(255)", nullable=False),
            Column("name", "VARCHAR(255)", nullable=False),
            Column("description", "TEXT"),
            Column("image_url", "TEXT"),
            Column("owner", "VARCHAR(255)"),
            Column("twitter_username", "VARCHAR(255)"),
            Column("contracts", "JSONB"),
            Column("created_at", "TIMESTAMP", nullable=False),
        ]

        table = Table(self.db, "opensea_collections", columns)
        table.create()
        return table

    def _check_rate_limit(self):
        current_time = time.time()

        if current_time - self.rate_limit_reset >= self.RATE_LIMIT_WINDOW:
            self.rate_limit_calls = 0
            self.rate_limit_reset = current_time

        if self.rate_limit_calls >= self.RATE_LIMIT_MAX:
            sleep_time = self.RATE_LIMIT_WINDOW - (current_time - self.rate_limit_reset)
            if sleep_time > 0:
                print(f"Rate limit reached. Waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                self.rate_limit_calls = 0
                self.rate_limit_reset = time.time()
        else:
            time.sleep(2)

        self.rate_limit_calls += 1

    def extract(self, chain: str = "ethereum", limit: int = 50, max_workers: int = 5) -> List[Dict[str, Any]]:
        def fetch_page(page: int) -> List[Dict[str, Any]]:
            params = {
                "chain": chain,
                "limit": limit,
                "offset": page * limit
            }

            try:
                self._check_rate_limit()

                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()

                response_data = response.json()
                collections = response_data.get("collections", [])

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                debug_file = os.path.join(self.raw_data_path, f"raw_response_page_{page}_{timestamp}.txt")
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(response.text)

                return collections

            except requests.exceptions.RequestException as e:
                print(f"API request error for page {page}: {str(e)}")
                return []
            except Exception as e:
                print(f"Unexpected error during extraction for page {page}: {str(e)}")
                return []

        all_collections = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_page, page) for page in range(max_workers)]

            for future in as_completed(futures):
                collections = future.result()
                if collections:
                    all_collections.extend(collections)

        if all_collections:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_file_path = os.path.join(self.raw_data_path, f"opensea_collections_{timestamp}.json")
            with open(json_file_path, "w", encoding="utf-8") as f:
                json.dump(all_collections, f, indent=2, ensure_ascii=False)

            csv_file_path = os.path.join(self.raw_data_path, f"opensea_collections_{timestamp}.csv")
            df = pd.DataFrame(all_collections)
            df.to_csv(csv_file_path, index=False, encoding="utf-8")

        print(f"Successfully extracted {len(all_collections)} collections")
        return all_collections

    @staticmethod
    def transform(collections: List[Dict[str, Any]], max_workers: int = 5) -> List[Dict[str, Any]]:
        def process_collection(collection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            try:
                name = str(collection.get("name", "")).strip()
                description = str(collection.get("description", ""))
                if description:
                    description = description.strip()
                    description = description[:5000] if len(description) > 5000 else description

                image_url = str(collection.get("image_url", "")).strip()
                if image_url and not (image_url.startswith("http://") or image_url.startswith("https://")):
                    image_url = ""

                twitter_username = str(collection.get("twitter_username", ""))
                if twitter_username:
                    twitter_username = twitter_username.strip().lstrip("@")

                owner = str(collection.get("owner", "")).strip()

                contracts = collection.get("contracts", [])
                if isinstance(contracts, list):
                    contracts = [
                        {
                            "address": contract.get("address"),
                            "chain": contract.get("chain")
                        }
                        for contract in contracts
                        if isinstance(contract, dict) and
                           contract.get("chain") == "ethereum" and
                           contract.get("address")
                    ]
                else:
                    contracts = []

                collection_name = str(collection.get("collection", "")).strip()
                if not name or not collection_name:
                    print(f"Skipping collection due to missing required data: {collection}")
                    return None

                transformed = {
                    "collection": collection_name,
                    "name": name,
                    "description": description,
                    "image_url": image_url,
                    "owner": owner,
                    "twitter_username": twitter_username,
                    "contracts": json.dumps(contracts, ensure_ascii=False),
                    "created_at": datetime.now().isoformat()
                }
                print(f"Successfully transformed collection: {name}")
                return transformed

            except Exception as e:
                print(f"Error transforming collection: {str(e)}")
                print(f"Problematic collection data: {collection}")
                return None

        transformed_data = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_collection, collection) for collection in collections]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    transformed_data.append(result)

        print(f"Successfully transformed {len(transformed_data)} collections")
        return transformed_data

    def aggregate_data(self, collections: List[Dict[str, Any]]) -> Dict[str, Any]:
        aggregated_data = {
            "total_collections": len(collections),
            "collections_with_twitter": sum(1 for c in collections if c.get("twitter_username")),
            "collections_by_owner": {},
            "contract_counts": {},
            "categories": {},
            "timestamp": datetime.now().isoformat()
        }

        for collection in collections:
            owner = collection.get("owner", "unknown")
            aggregated_data["collections_by_owner"][owner] = aggregated_data["collections_by_owner"].get(owner, 0) + 1

            contracts = collection.get("contracts", [])
            for contract in contracts:
                if isinstance(contract, dict):
                    chain = contract.get("chain", "unknown")
                    aggregated_data["contract_counts"][chain] = aggregated_data["contract_counts"].get(chain, 0) + 1

            category = collection.get("category", "uncategorized")
            if category:
                aggregated_data["categories"][category] = aggregated_data["categories"].get(category, 0) + 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        agg_file_path = os.path.join(self.raw_data_path, f"aggregation_{timestamp}.json")
        with open(agg_file_path, "w", encoding="utf-8") as f:
            json.dump(aggregated_data, f, indent=2, ensure_ascii=False)

        return aggregated_data

    @staticmethod
    def load(data: List[Dict[str, Any]], table: Table):
        try:
            table.bulk_insert(data)
            print(f"Successfully loaded {len(data)} collections into the database")
        except Exception as e:
            print(f"Error loading data into database: {str(e)}")

    def run_pipeline(self, chain: str = "ethereum", limit: int = 50, max_workers: int = 5):
        print("Starting ETL pipeline...")

        table = self._create_collections_table()

        print("Extracting data from OpenSea API...")
        raw_data = self.extract(chain, limit, max_workers)

        if not raw_data:
            print("No data extracted. Pipeline stopped.")
            return

        print("Transforming data...")
        transformed_data = self.transform(raw_data, max_workers)

        print("Aggregating data...")
        aggregated_data = self.aggregate_data(raw_data)
        print(f"Found {aggregated_data['total_collections']} collections")
        print(f"Collections with Twitter: {aggregated_data['collections_with_twitter']}")

        print("Loading data into database...")
        self.load(transformed_data, table)

        print("ETL pipeline completed successfully!")
