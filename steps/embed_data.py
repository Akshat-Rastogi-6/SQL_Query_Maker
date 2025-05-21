import logging
import os
from typing import Dict, Any, List
from zenml import step
from src.data_embedding import GoogleEmbedding

@step
def embed_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Embed the processed data and save to file."""
    try:
        logging.info("Embedding data...")
        
        # Initialize the embedding class
        embedder = GoogleEmbedding()
        
        # Embed the metadata
        embedded_data = embedder.embed_data(data)
        
        # Save to pkl file
        output_dir = os.path.join(os.getcwd(), "data", "embeddings")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "table_embeddings.pkl")
        embedder.save_embeddings(embedded_data, output_file)
        
        logging.info(f"Successfully embedded data for {len(embedded_data)} tables")
        return embedded_data
    except Exception as e:
        logging.error(f"Error embedding data: {e}")
        raise e
    
@step
def embedding_query(query: str) -> List[float]:
    """
    Embeds the query using the Google embedding strategy.
    
    Args:
        query (str): The query to be embedded.
        
    Returns:
        List[float]: The embedding vector for the query
    """
    try:
        logging.info("Embedding query...")
        
        # Initialize the embedding class
        embedder = GoogleEmbedding()
        
        # Embed the query
        embedding = embedder.embed_query(query)
        
        logging.info("Query embedding complete.")
        return embedding
    except Exception as e:
        logging.error(f"Error embedding query: {e}")
        raise e