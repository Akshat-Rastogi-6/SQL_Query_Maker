from zenml import pipeline
import logging
from typing import List

from steps.embed_data import embedding_query
from steps.search_embedding import search_embedding
from steps.response import response

@pipeline
def test_database_pipeline(query: str, include_tables: List[str]):
    """
    Test pipeline to validate the functionality of the training pipeline.
    Args:
        query (str): The query to the file.
    """
    query_embedding = embedding_query(query=query)
    query_embedding_search = search_embedding(query_embedding=query_embedding)
    res = response(matching_chunks=query_embedding_search, include_tables=include_tables, query=query)
    logging.info(f"Response: {res}")
    return res