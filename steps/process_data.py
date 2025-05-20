import logging
from typing import Dict, Any, List, NamedTuple
from zenml import step

from src.metaDataGeneration import GeminiMetaDataCreation

class ProcessOutput(NamedTuple):
    """Output type for process_data step."""
    tables: List[str]
    schemas: Dict[str, Any]

@step
def process_data(data: Dict[str, Any]) -> dict:
    """Process the data retrieved from the database.
    
    Args:
        data: Dictionary containing tables and schemas
        
    Returns:
        ProcessOutput: A named tuple containing tables and their schemas
    """
    try:
        tables = data["tables"]
        table_schemas = data["schemas"]

        # debugging step  
        logging.info(f"Tables found: {tables}")
        # logging.info(f"Table schemas: {table_schemas}")
                
        # Generate metadata using Gemini API
        metadata_generator = GeminiMetaDataCreation(tables=tables, schemas=table_schemas)
        metadata = metadata_generator.generate_metadata()

        return metadata
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise e