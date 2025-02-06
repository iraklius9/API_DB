import sqlite3
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

@dataclass
class Column:
    name: str
    data_type: str
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False

class Database:
    def __init__(self, database: str):
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = sqlite3.connect(self.database)
            self.cursor = self.connection.cursor()
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute(self, query: str, params: tuple = None) -> Optional[List[tuple]]:
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            self.connection.commit()
            return None
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Query execution failed: {str(e)}")

class Table:
    PG_TO_SQLITE_TYPES = {
        "SERIAL": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "INTEGER": "INTEGER",
        "TEXT": "TEXT",
        "VARCHAR": "TEXT",
        "TIMESTAMP": "TEXT",
        "JSONB": "TEXT",
        "BOOLEAN": "INTEGER"
    }

    def __init__(self, db: Database, table_name: str, columns: List[Column]):
        self.db = db
        self.table_name = table_name
        self.columns = columns

    def _convert_type(self, pg_type: str) -> str:
        pg_type = pg_type.upper()
        for pg_prefix, sqlite_type in self.PG_TO_SQLITE_TYPES.items():
            if pg_type.startswith(pg_prefix):
                return sqlite_type
        return "TEXT"

    def create(self):
        columns_def = []
        for col in self.columns:
            sqlite_type = self._convert_type(col.data_type)
            if col.primary_key and "PRIMARY KEY" not in sqlite_type:
                sqlite_type += " PRIMARY KEY"
            if not col.nullable:
                sqlite_type += " NOT NULL"
            if col.unique:
                sqlite_type += " UNIQUE"
            columns_def.append(f"{col.name} {sqlite_type}")

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {', '.join(columns_def)}
        )
        """
        self.db.execute(query)

    def drop(self):
        query = f"DROP TABLE IF EXISTS {self.table_name}"
        self.db.execute(query)

    def add_column(self, column: Column):
        query = f"""
        ALTER TABLE {self.table_name}
        ADD COLUMN {column.name} {self._convert_type(column.data_type)}
        """
        self.db.execute(query)

    def remove_column(self, column_name: str):
        raise NotImplementedError("SQLite doesn't support dropping columns")

    def change_column_type(self, column_name: str, new_type: str):
        raise NotImplementedError("SQLite doesn't support changing column types")

    def insert(self, data: Dict[str, Any]):
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        query = f"""
        INSERT INTO {self.table_name} ({columns})
        VALUES ({placeholders})
        """
        self.db.execute(query, tuple(data.values()))

    def bulk_insert(self, data: List[Dict[str, Any]]):
        if not data:
            return

        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        query = f"""
        INSERT INTO {self.table_name} ({", ".join(columns)})
        VALUES ({placeholders})
        """
        
        for row in data:
            values = tuple(row[col] for col in columns)
            self.db.execute(query, values)

    def select(self, columns: List[str] = None, where: str = None, 
               order_by: str = None, limit: int = None) -> List[tuple]:
        columns_str = "*" if not columns else ", ".join(columns)
        query = f"SELECT {columns_str} FROM {self.table_name}"
        
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        return self.db.execute(query)

    def select_like(self, column: str, pattern: str) -> List[tuple]:
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE {column} LIKE ?
        """
        return self.db.execute(query, (pattern,))

    def select_in(self, column: str, values: List[Any]) -> List[tuple]:
        placeholders = ", ".join(["?" for _ in values])
        query = f"""
        SELECT * FROM {self.table_name}
        WHERE {column} IN ({placeholders})
        """
        return self.db.execute(query, tuple(values))

    def update(self, data: Dict[str, Any], where: str):
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        query = f"""
        UPDATE {self.table_name}
        SET {set_clause}
        WHERE {where}
        """
        self.db.execute(query, tuple(data.values()))

    def delete(self, where: str):
        query = f"""
        DELETE FROM {self.table_name}
        WHERE {where}
        """
        self.db.execute(query)
