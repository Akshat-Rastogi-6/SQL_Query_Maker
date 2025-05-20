from zenml import pipeline
from steps.databaseConnect import connectTheDatabase
from steps.process_data import process_data
from steps.embed_data import embed_data

@pipeline(enable_cache=False)
def train_database_pipeline(
    password: str,
    database_name: str,
    host: str,
    user: str
):
    """Database training pipeline.
    
    Args:
        password (str): The password for the database connection.
        database_name (str): The name of the database.
        host (str): The host of the database.
        user (str): The user for the database connection.
    """
    data = connectTheDatabase(
        password=password, 
        database_name=database_name, 
        host=host, 
        user=user
    )
    output = process_data(data=data)
    embedding = embed_data(data=output)
    