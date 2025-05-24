import logging
from typing import Dict, List, Any

from zenml import step
from src.databaseConnection import IngestData

@step
def connectTheDatabase(
    password: str,
    database_name: str,
    host: str,
    user: str
) -> Dict[str, Any]:
    """Connects to the database and fetches all necessary data.
    
    Args:
        password: Database password
        database_name: Database name
        host: Database host
        user: Database username
        
    Returns:
        Dict containing tables and their schemas
    """
    try:
        logging.info("Connecting to the database...")
        db_connection = IngestData(password=password, database_name=database_name, host=host, user=user)
        connection = db_connection.connect_to_database()
        if connection is None:
            logging.error("Failed to connect to the database.")
            return {"tables": [], "schemas": {}}
            
        # Get data directly in this step
        logging.info("Fetching tables from the database...")
        tables = db_connection.fetch_tables(connection, database_name)
        if tables is None:
            logging.error("Failed to fetch tables.")
            return {"tables": [], "schemas": {}}
            
        # Get schemas
        table_schemas = {}
        for table in tables:
            schema = db_connection.fetch_table_schemas(connection, table)
            if schema is None:
                logging.error(f"Failed to fetch schema for table: {table}")
                continue
            table_schemas[table] = schema
            
        # Close connection
        connection.close()
        
        return {"tables": tables, "schemas": table_schemas}
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return {"tables": [], "schemas": {}}


