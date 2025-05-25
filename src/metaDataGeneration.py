import logging

from abc import ABC, abstractmethod
import google.generativeai as genai
from dotenv import load_dotenv
import os
import json
load_dotenv('.env')

class MetaDataGeneration(ABC):
    """
    Abstract base class for generating metadata for tables and their schemas.
    """
    @abstractmethod
    def generate_metadata(self) -> dict:
        """
        Generates metadata for the tables and their schemas.
        
        Returns:
            dict: A dictionary containing metadata information.
        """
        pass

class GeminiMetaDataCreation(MetaDataGeneration):
    """
    Class for generating metadata using Gemini API.
    """

    def __init__(self, tables: list, schemas: dict):
        """
        Initializes the GeminiMetaDataCreation class.
        
        Args:
            tables (list): List of table names.
            schemas (dict): Dictionary containing table schemas.
        """
        self.tables = tables
        self.schemas = schemas

    def generate_metadata(self) -> dict:
        """
        Generates metadata for the tables and their schemas using Gemini API.
        
        Returns:
            dict: A dictionary containing metadata information.
        """
        try:
            # Initialize the Gemini API client
            genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

            # Generate metadata for each table
            output_format = """
                {{
                "table_name": "string",
                "schema_description": "string",
                "columns": [
                    {{
                    "name": "string",
                    "type": "string",
                    "description": "string"
                    }}
                ],
                "embedding_text": "string"
                }}
                """
            example = """
                    {
                        "table_name": "users",
                        "schema_description": "A table to store user details including identity, name, contact, and creation timestamp.",
                        "columns": [
                            {
                            "name": "id",
                            "type": "INTEGER",
                            "description": "Unique identifier for each user."
                            },
                            {
                            "name": "name",
                            "type": "VARCHAR(100)",
                            "description": "Full name of the user."
                            },
                            {
                            "name": "email",
                            "type": "VARCHAR(100)",
                            "description": "Optional email address of the user."
                            },
                            {
                            "name": "created_at",
                            "type": "TIMESTAMP",
                            "description": "Timestamp when the user record was created."
                            }
                        ],
                        "embedding_text": "The 'users' table contains user details including an ID, full name, optional email address, and a creation timestamp."
                        }
                """
            restrictions = """
            1. "```json\n{\n  \"table_name\": \"employees\",\n  \"schema_description\": \"This table stores information about employees, including their ID, name, salary, and bonus.\",\n  \"columns\": [\n    {\n      \"name\": \"employee_id\",\n      \"type\": \"int\",\n      \"description\": \"Unique identifier for each employee.\"\n    },\n    {\n      \"name\": \"name\",\n      \"type\": \"varchar(100)\",\n      \"description\": \"Employee's name.\"\n    },\n    {\n      \"name\": \"salary\",\n      \"type\": \"decimal(10,2)\",\n      \"description\": \"Employee's salary.\"\n    },\n    {\n      \"name\": \"bonus\",\n      \"type\": \"decimal(10,2)\",\n      \"description\": \"Employee's bonus.\"\n    }\n  ],\n  \"embedding_text\": \"The 'employees' table stores employee data, including a unique employee ID, employee name, salary, and bonus amount.\"\n}\n```\n"
            2. "```json\n{\n  \"table_name\": \"products\",\n  \"schema_description\": \"This table contains product information, including product ID, name, price, and stock quantity.\",\n  \"columns\": [\n    {\n      \"name\": \"product_id\",\n      \"type\": \"int\",\n      \"description\": \"Unique identifier for each product.\"\n    },\n    {\n      \"name\": \"product_name\",\n      \"type\": \"varchar(100)\",\n      \"description\": \"Name of the product.\"\n    },\n    {\n      \"name\": \"price\",\n      \"type\": \"decimal(10,2)\",\n      \"description\": \"Price of the product.\"\n    },\n    {\n      \"name\": \"stock_quantity\",\n      \"type\": \"int\",\n      \"description\": \"Available stock quantity of the product.\"\n    }\n  ],\n  \"embedding_text\": \"The 'products' table contains product details, including a unique product ID, product name, price, and available stock quantity.\"\n}\n```\n" 


            Inshort, the output should be a JSON object with the following structure:
            {
                "table_name": "string",
                "schema_description": "string",
                "columns": [
                    {
                    "name": "string",
                    "type": "string",
                    "description": "string"
                    }
                ],
                "embedding_text": "string"
            }       
            """
            metadata = {}
            for table in self.tables:
                schema = self.schemas.get(table, {})
                prompt = f"""
                    [INSTRUCTION]
                    You are a database schema metadata generator. Based on the provided schema definition, generate a JSON object that includes structured metadata and a natural-language description of the table.
                    Please stick to the data format and structure provided in the example.
                    The information like table name and column names should be extracted from the schema definition. It should be correct. 


                    [CONTEXT]
                    - The output must be a valid JSON object (no markdown formatting, no code fences).
                    - You must include: table name, a short description of the table, a list of columns with name, type, and description, and a flattened natural-language summary of the table for use in embedding.
                    - Keep column descriptions concise and meaningful.
                    - The "embedding_text" field should describe the table and all its columns in a readable sentence.

                    [INPUT DATA]
                    Table name: {table}
                    DDL:
                    {schema}


                    [OUTPUT FORMAT]
                    {output_format}

                    [RESTRICTIONS]
                    {restrictions}

                    [EXAMPLE]
                    {example}

                """
                try:
                    llm = genai.GenerativeModel('gemini-1.5-flash')
                    response = llm.generate_content(prompt)
                except Exception as e:
                    logging.warning(f"API error occurred with primary key: {e}")
                    
                    # Try backup API keys
                    success = False
                    for i in range(2, 6):  # Try API keys 2 through 5
                        try:
                            # Reconfigure with next backup API key
                            logging.info(f"Attempting to use backup API key {i}")
                            genai.configure(api_key=os.getenv(f'GEMINI_API_KEY{i}'))
                            llm = genai.GenerativeModel('gemini-1.5-flash')
                            response = llm.generate_content(prompt)
                            logging.info(f"Successfully used backup API key {i}")
                            success = True
                            break
                        except Exception as backup_error:
                            logging.warning(f"Failed with API key {i}: {backup_error}")
                    
                    if not success:
                        logging.error("All API keys failed")
                        raise Exception("All Gemini API keys failed")
                    
                metadata[table] = response.text

                try:
                    # Save the current cumulative metadata to a JSON file.
                    # This file will be overwritten in each iteration.
                    # Ensure the 'json' module is imported at the top of your Python file (import json).
                    with open(f'data/chunk/{table}.json', 'w') as json_outfile:
                        json.dump(response.text, json_outfile, indent=4)
                    logging.info(f"Metadata for table {table} saved to {table}.json")
                except IOError as file_io_error:
                    logging.error(f"Failed to write metadata to metadata.json: {file_io_error}")

            return metadata
        except Exception as e:
            logging.error(f"Error generating metadata: {e}")
            return {}