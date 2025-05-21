import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import torch
import numpy as np
import faiss
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_core.documents import Document
import os
import json
import pickle
from pathlib import Path
from dotenv import load_dotenv

load_dotenv('.env')
# Load environment variables 

class DataEmbedding(ABC):
    """
    Abstract class for data embedding strategies.
    """

    @abstractmethod
    def embed_data(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Abstract method to embed the data.
        
        Args:
            metadata: Dictionary containing the metadata for tables
            
        Returns:
            Dictionary with embedded metadata
        """
        pass
    
    @abstractmethod
    def embed_query(self, query: str) -> List[float]:
        """
        Abstract method to embed a query string.
        
        Args:
            query: The query string to embed
            
        Returns:
            List[float]: Embedding vector
        """
        pass
    
    @abstractmethod
    def save_embeddings(self, embedded_metadata: Dict[str, Any], filename: str) -> bool:
        """
        Abstract method to save embeddings to disk.
        
        Args:
            embedded_metadata: Dictionary containing embedded metadata
            filename: Name of the file to save embeddings to
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass

class GoogleEmbedding(DataEmbedding):
    """
    Google embedding strategy.
    """
    
    def __init__(self):
        """Initialize the embedding model."""
        GOOGLE_API_KEY = os.getenv('GEMINI_API_KEY')   
        self.model = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key= GOOGLE_API_KEY)

    def embed_data(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Embeds metadata using Google's embedding model.
        
        Args:
            metadata: Dictionary with table metadata where keys are table names
            
        Returns:
            Dictionary with the same structure but with embeddings added
        """
        try:
            # Create a new dictionary to store results
            embedded_metadata = {}
            
            # Process each table's metadata
            for table_name, table_data in metadata.items():
                logging.info(f"Embedding data for table: {table_name}")
                
                # If table_data is a string (likely JSON string), parse it
                if isinstance(table_data, str):
                    try:
                        table_data = json.loads(table_data)
                    except json.JSONDecodeError:
                        logging.warning(f"Could not parse JSON for table {table_name}")
                        # Create a basic dictionary for unparseable strings
                        table_data = {"raw_text": table_data, "table_name": table_name}
                
                # Get the text to embed - prioritize "embedding_text" if available
                if isinstance(table_data, dict):
                    text_to_embed = table_data.get("embedding_text", "")
                    
                    # If no embedding_text, create one from other metadata
                    if not text_to_embed:
                        table_desc = table_data.get("schema_description", "")
                        table_name_from_data = table_data.get("table_name", table_name)
                        text_to_embed = f"Table {table_name_from_data}: {table_desc}"
                else:
                    # Handle case where table_data is still not a dict
                    text_to_embed = str(table_data)
                
                # Generate embedding if we have text
                if text_to_embed and isinstance(text_to_embed, str):
                    try:
                        embedding = self.model.embed_query(text_to_embed)
                        
                        # Clone the table data to avoid modifying original
                        if isinstance(table_data, dict):
                            embedded_table = table_data.copy()
                        else:
                            embedded_table = {"raw_data": str(table_data)}
                            
                        embedded_table["embedding"] = embedding
                        embedded_metadata[table_name] = embedded_table
                        
                    except Exception as e:
                        logging.error(f"Error embedding table {table_name}: {e}")
                        embedded_metadata[table_name] = table_data  # Keep original data
                else:
                    logging.warning(f"No valid text to embed for table {table_name}")
                    embedded_metadata[table_name] = table_data  # Keep original data
                    
            return embedded_metadata
            
        except Exception as e:
            logging.error(f"Error in embedding process: {e}")
            return metadata  # Return original metadata on error
    
    def embed_query(self, query: str) -> List[float]:
        """
        Embeds a query string using Google's embedding model.
        
        Args:
            query: The query string to embed
            
        Returns:
            List[float]: The embedding vector for the query
        """
        try:
            # Embed the query
            embedding = self.model.embed_query(query)
            return embedding
        except Exception as e:
            logging.error(f"Error embedding query: {e}")
            raise e
    
    def save_embeddings(self, embedded_metadata: Dict[str, Any], filename: str) -> bool:
        """
        Saves embedded metadata to a pickle file and creates a FAISS index for vector search.
        
        Args:
            embedded_metadata: Dictionary containing embedded metadata
            filename: Name of the file to save embeddings to
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            directory = os.path.dirname(filename)
            if directory:
                Path(directory).mkdir(parents=True, exist_ok=True)
                
            # 1. Save the complete metadata as pickle
            with open(filename, 'wb') as f:
                pickle.dump(embedded_metadata, f)
                
            # 2. Create FAISS index from embeddings
            embeddings = []
            table_ids = []  # Keep track of which embedding belongs to which table
            
            # Extract embeddings from metadata
            for i, (table_name, table_data) in enumerate(embedded_metadata.items()):
                if isinstance(table_data, dict) and "embedding" in table_data:
                    embedding = table_data["embedding"]
                    if isinstance(embedding, list) and len(embedding) > 0:
                        embeddings.append(embedding)
                        table_ids.append(table_name)
            
            if embeddings:
                # Convert to numpy array
                embeddings_array = np.array(embeddings, dtype=np.float32)
                
                # Get embedding dimension
                dimension = embeddings_array.shape[1]
                
                # Create a FAISS index - using L2 distance (Euclidean)
                index = faiss.IndexFlatL2(dimension)
                
                # Add vectors to the index
                index.add(embeddings_array)
                
                # Save the FAISS index
                faiss_filename = filename.replace('.pkl', '_faiss.index')
                faiss.write_index(index, faiss_filename)
                
                # Save the mapping of indices to table names
                mapping_filename = filename.replace('.pkl', '_mapping.json')
                with open(mapping_filename, 'w') as f:
                    json.dump(table_ids, f)
                    
                logging.info(f"FAISS index created with {len(embeddings)} vectors and saved to {faiss_filename}")
            else:
                logging.warning("No valid embeddings found to create FAISS index")
                
            logging.info(f"Embeddings saved to {filename}")
            return True
        except Exception as e:
            logging.error(f"Error saving embeddings to {filename}: {e}")
            logging.exception(e)
            return False