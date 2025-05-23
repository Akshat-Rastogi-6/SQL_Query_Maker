import logging
import os
from typing import List
from zenml import step

from src.data_response import Response, GeminiResponse

@step
def response(matching_chunks: List[str], query: str) -> str:
    try:
        logging.info("Preparing response...")
        
        # Check if the directory exists and create it if needed
        chunk_dir = os.path.join(os.getcwd(), "data", "chunk")
        if not os.path.exists(chunk_dir):
            os.makedirs(chunk_dir)
        
        # Debug: log which files we're looking for
        for table_name in matching_chunks:
            logging.info(f"Looking for table JSON: {table_name}.json")
        
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
        logging.info(f"Response: {type(resp)}")
        logging.info(f"Response: {resp}")
        response_file_path = os.path.join(chunk_dir, "response.txt")
        
        # After generating the response
        with open(response_file_path, "w") as f:
            f.write(resp)
        
        return resp
    except Exception as e:
        logging.error(f"Error during response generation: {e}") 
        raise e