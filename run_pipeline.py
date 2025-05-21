from zenml.client import Client
from pipelines.training_pipeline import train_database_pipeline
from pipelines.testing_pipeline import test_database_pipeline

if __name__ == "__main__":
    # Define the path to your data

    host='localhost'
    user='root'
    password='123456'
    database='practice'

    # Run the pipeline
    client = Client()

    # pipeline_run = train_database_pipeline(
    #     password=password,
    #     database_name=database,
    #     host=host,
    #     user=user
    # )

    # print(f"Pipeline run started with ID: {pipeline_run.id}")

    test_database_pipeline(
        query="Based on the sales schema write the query. write a query to return the employee id of all the users with quantity greater than 5."
    )