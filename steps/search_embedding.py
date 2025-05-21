import faiss
import logging
import numpy as np
import json
import os
from typing import List, Dict, Any
from zenml import step

@step
def search_embedding(query_embedding: List[float], top_k: int = 3) -> List[str]:
    """
    Find tables similar to a query embedding using the FAISS index.
    
    Args:
        query_embedding: Embedding vector to find similar tables for
        top_k: Number of top results to return
        
    Returns:
        List of table names most similar to the query
    """
    try:
        # Define paths
        base_dir = os.path.join(os.getcwd(), "data", "embeddings")
        index_path = os.path.join(base_dir, "table_embeddings_faiss.index")
        mapping_path = os.path.join(base_dir, "table_embeddings_mapping.json")
        
        # Check if files exist
        if not os.path.exists(index_path) or not os.path.exists(mapping_path):
            logging.error(f"Index or mapping file not found at {index_path} or {mapping_path}")
            return []
        
        # Convert to numpy array - no need to embed again
        query_vector = np.array([query_embedding], dtype=np.float32)
        
        # Load the FAISS index
        index = faiss.read_index(index_path)
        
        # Load the mapping
        with open(mapping_path, 'r') as f:
            table_mapping = json.load(f)
            
        # Search the index
        distances, indices = index.search(query_vector, min(top_k, len(table_mapping)))
        
        # Get the table names
        similar_tables = [table_mapping[int(idx)] for idx in indices[0] if idx >= 0 and idx < len(table_mapping)]
        
        logging.info(f"Found similar tables: {similar_tables}")
        return similar_tables
    except Exception as e:
        logging.error(f"Error finding similar tables: {e}")
        return []