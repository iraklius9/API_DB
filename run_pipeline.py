from opensea_etl.etl import OpenSeaETL
from custom_orm.base import Database
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    
    api_key = os.getenv("OPENSEA_API_KEY")
    
    if not api_key:
        print("Error: OPENSEA_API_KEY not found in environment variables")
        return
    
    try:
        db = Database("opensea.db")
        db.connect()
        print("Successfully connected to SQLite database")
        
        etl = OpenSeaETL(api_key=api_key, db=db)
        etl.run_pipeline(chain="ethereum", limit=50)
        
    except Exception as e:
        print(f"Error running pipeline: {str(e)}")
    finally:
        if db and db.connection:
            db.disconnect()
            print("Database connection closed")

if __name__ == "__main__":
    main()
