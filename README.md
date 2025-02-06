# OpenSea ETL Pipeline with Custom ORM

This project implements an ETL (Extract, Transform, Load) pipeline for OpenSea NFT collections data using a custom-built ORM system.

## Features

- Custom ORM supporting CRUD operations
- ETL pipeline for OpenSea Collections API
- Raw data storage in JSON format
- PostgreSQL database integration

## Project Structure

```
API_DB/
├── custom_orm/           # Custom ORM implementation
│   ├── __init__.py
│   └── base.py          # Core ORM functionality
├── opensea_etl/         # OpenSea ETL pipeline
│   ├── __init__.py
│   └── etl.py          # ETL implementation
├── raw_data/           # Directory for storing raw JSON data
├── .env.example        # Example environment variables
├── requirements.txt    # Project dependencies
└── README.md          # Project documentation
```

## Setup

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file:
- Copy `.env.example` to `.env`
- Fill in your database credentials and OpenSea API key

## Usage

1. First, ensure you have PostgreSQL installed and running.

2. Get your OpenSea API key from [OpenSea API Keys Documentation](https://docs.opensea.io/reference/api-keys)

3. Run the ETL pipeline:

```python
from opensea_etl.etl import OpenSeaETL
from custom_orm.base import Database
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize database connection
db = Database(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 5432))
)

# Connect to database
db.connect()

# Initialize and run ETL pipeline
etl = OpenSeaETL(api_key=os.getenv("OPENSEA_API_KEY"), db=db)
etl.run_pipeline(chain="ethereum", limit=50)

# Disconnect from database
db.disconnect()
```

## Custom ORM Features

The custom ORM supports:

- Database connection management
- Table creation and deletion
- Column management (add, remove, modify)
- CRUD operations
- Filtering and sorting
- Bulk operations

## Data Model

The OpenSea collections are stored with the following schema:

- `id`: Serial primary key
- `collection`: Collection name
- `name`: Item name
- `description`: Collection description
- `image_url`: Image URL
- `owner`: Owner username
- `twitter_username`: Twitter username
- `contracts`: Contract details (stored as JSONB)
- `created_at`: Timestamp of data insertion

## Raw Data Storage

Raw data from the OpenSea API is stored in JSON format in the `raw_data` directory. Each file is named with a timestamp for easy tracking and reference.

## Error Handling

The ETL pipeline includes error handling for:
- API request failures
- Database connection issues
- Data transformation errors
- Loading failures

## Rate Limiting

Be mindful of OpenSea API rate limits when running the pipeline. Adjust the `limit` parameter accordingly.
