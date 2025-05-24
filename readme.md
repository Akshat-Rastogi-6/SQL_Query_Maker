# SQL Query Assistant

A natural language to SQL query converter that utilizes large language models to help users interact with MySQL databases more efficiently.

## Overview

SQL Query Assistant is a tool that bridges the gap between natural language and SQL queries, allowing users without extensive SQL knowledge to query databases effectively. The application connects to MySQL databases, understands their schema, and generates appropriate SQL queries based on user questions.

## Features

- **Database Connection**: Connect to MySQL databases with authentication
- **Schema Understanding**: Automatically extracts and understands database schema
- **Natural Language Processing**: Convert plain English questions to SQL queries
- **Interactive UI**: User-friendly Streamlit interface for querying and viewing results
- **Query Execution**: Execute generated SQL and display results
- **Table Selection**: Select specific tables to include in the query context
- **Schema Visualization**: View table schemas and sample data

## Prerequisites

- Python 3.10+
- MySQL server
- Google AI API key for Gemini model access

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/sql-query-assistant.git
cd sql-query-assistant
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root directory with:
```
GEMINI_API_KEY=your_gemini_api_key
```

## Project Structure

```
sql-query-assistant/
├── .zen/                  # ZenML configuration
├── data/                  # Data storage (generated at runtime)
│   ├── chunk/             # Table metadata chunks
│   └── embeddings/        # Vector embeddings
├── pipelines/             # ZenML pipelines
├── src/                   # Source code
│   ├── data_embedding.py  # Embedding generation
│   ├── data_response.py   # Response generation
│   ├── databaseConnection.py  # Database connection logic
│   └── metaDataGeneration.py  # Schema metadata extraction
├── steps/                 # ZenML pipeline steps
├── test/                  # Test files and examples
│   ├── scripts/           # Example scripts
│   └── 1.ipynb            # Example notebook
├── run_pipeline.py        # Pipeline execution
├── run_deployment.py      # Streamlit application
└── README.md              # This file
```

## Workflow

The SQL Query Assistant works in two main phases:

### Training Phase

1. **Database Connection**: Connect to a MySQL database
2. **Schema Extraction**: Extract tables and their schemas
3. **Metadata Generation**: Generate descriptive metadata for each table using AI
4. **Embedding Creation**: Create vector embeddings for each table's metadata
5. **Index Building**: Build a FAISS index for similarity search

### Query Phase

1. **Natural Language Input**: User inputs a question in plain English
2. **Context Building**: Selected tables are included as context
3. **Semantic Search**: Find relevant tables using vector similarity
4. **Query Generation**: Generate a SQL query based on the question and context
5. **Query Execution**: Execute the SQL query on the database
6. **Response Formatting**: Format and display results to the user

## Usage

1. Start the application:
```bash
streamlit run run_deployment.py
```

2. Connect to your MySQL database using the sidebar
3. Select the database you want to query
4. Train the model on your database schema
5. Select tables you want to include in your query context
6. Enter your question in natural language
7. Review the generated SQL and results

## Example Queries

- "Show me all customers who made purchases last month"
- "What are the top 5 best-selling products?"
- "Find employees with sales greater than $10,000"
- "Count the number of orders by country"

## Technologies Used

- **ZenML**: Pipeline management
- **Streamlit**: Web interface
- **PyMySQL**: Database connection
- **Google Gemini**: AI for SQL generation
- **FAISS**: Vector similarity search
- **LangChain**: LLM framework components

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.