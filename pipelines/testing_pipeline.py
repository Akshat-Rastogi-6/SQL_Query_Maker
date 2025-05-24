from zenml import pipeline
import logging

from steps.embed_data import embedding_query
from steps.search_embedding import search_embedding
from steps.response import response

@pipeline(enable_cache=False)
def test_database_pipeline(query: str):
    """
    Test pipeline to validate the functionality of the training pipeline.
    Args:
        query (str): The query to the file.
    """
    query_embedding = embedding_query(query=query)
    query_embedding_search = search_embedding(query_embedding=query_embedding)
    res = response(matching_chunks=query_embedding_search, query=query)
    logging.info(f"Response: {res}")
    return res