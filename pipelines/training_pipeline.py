from zenml import pipeline
from steps.databaseConnect import connectTheDatabase
from steps.ingest_data import process_data

@pipeline
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
    # Access via output.tables and output.schemas
    return output