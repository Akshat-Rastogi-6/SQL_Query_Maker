import logging
import os
from typing import List
from zenml import step

from src.data_response import Response, GeminiResponse

@step
def response(matching_chunks: List[str], query: str) -> str:
    try:
        logging.info("Preparing response...")
        
        # Load the actual table metadata from files
        content_chunks = []
        for table_name in matching_chunks:
            try:
                # Load the JSON file for this table
                json_path = os.path.join(os.getcwd(), "data", "chunk", f"{table_name}.json")
                if os.path.exists(json_path):
                    with open(json_path, 'r') as f:
                        content_chunks.append(f.read())
            except Exception as e:
                logging.error(f"Could not load metadata for table {table_name}: {e}")
        
        agent1 = GeminiResponse()
        resp = agent1.get_response(matching_chunks=content_chunks, query=query)
        logging.info(f"Response: {resp}")
        return resp
    except Exception as e:
        logging.error(f"Error during response generation: {e}") 
        raise e