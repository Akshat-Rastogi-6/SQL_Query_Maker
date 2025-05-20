import logging
from typing import Dict, Any, List, NamedTuple
from zenml import step

class ProcessOutput(NamedTuple):
    """Output type for process_data step."""
    tables: List[str]
    schemas: Dict[str, Any]

@step
def process_data(data: Dict[str, Any]) -> ProcessOutput:
    """Process the data retrieved from the database.
    
    Args:
        data: Dictionary containing tables and schemas
        
    Returns:
        ProcessOutput: A named tuple containing tables and their schemas
    """
    try:
        tables = data["tables"]
        table_schemas = data["schemas"]
        
        logging.info(f"Tables found: {tables}")
        logging.info(f"Table schemas: {table_schemas}")
                
        return ProcessOutput(tables=tables, schemas=table_schemas)
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return ProcessOutput(tables=[], schemas={})