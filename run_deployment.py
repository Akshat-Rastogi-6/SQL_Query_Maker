import streamlit as st
import pymysql
from pipelines.testing_pipeline import test_database_pipeline
from pipelines.training_pipeline import train_database_pipeline
import pandas as pd

# --- Database Connection Functions ---

def authenticate_mysql(host, user, password, port):
    """
    Authenticate with MySQL server and get available databases
    """
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=int(port)
        )
        
        # Get list of databases
        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
        
        # Store connection and databases in session state
        st.session_state.connection = connection
        st.session_state.authenticated = True
        st.session_state.databases = databases
        
        st.sidebar.success("Successfully connected to MySQL server!")
        
    except Exception as e:
        st.sidebar.error(f"Connection failed: {str(e)}")
        st.session_state.authenticated = False

def connect_to_database(host, user, password, port, database):
    """
    Connect to a specific database
    """
    try:
        # Close previous connection
        if 'db_connection' in st.session_state:
            st.session_state.db_connection.close()
        
        # Clear previous database information if changing databases
        if 'current_db' in st.session_state:
            if st.session_state.current_db != database:
                if 'tables' in st.session_state:
                    del st.session_state.tables
                if 'selected_tables' in st.session_state:
                    del st.session_state.selected_tables
        
        # Connect to the selected database
        db_connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=int(port),
            database=database
        )
        
        # Store the database connection
        st.session_state.db_connection = db_connection
        st.session_state.current_db = database
        
        st.sidebar.success(f"Connected to database: {database}")
        
    except Exception as e:
        st.sidebar.error(f"Failed to connect to database: {str(e)}")

# --- Training & Table Functions ---

def train_model(password, database, host, user):
    """
    Train the model on current database
    """
    with st.spinner("Training model with database data..."):
        try:
            # Call the training pipeline with the connected database parameters
            train_database_pipeline(
                password=password,
                database_name=database,
                host=host,
                user=user
            )
            st.sidebar.success("Model training completed successfully!")
            
            # Get table names after successful model training
            with st.session_state.db_connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                st.session_state.tables = [table[0] for table in cursor.fetchall()]
            
        except Exception as e:
            st.sidebar.error(f"Model training failed: {str(e)}")

def display_table_selection():
    """
    Display and handle table selection
    """
    selected_tables = st.multiselect(
        "Select tables to include in your query context:",
        options=st.session_state.tables,
        default=[] # Always start with empty selection for consistent behavior
    )
    
    # Store selected tables in session state
    if selected_tables:
        st.session_state.selected_tables = selected_tables
        st.success(f"Selected {len(selected_tables)} tables")
    else:
        # Clear selected_tables if user deselects all tables
        if 'selected_tables' in st.session_state:
            del st.session_state.selected_tables
    
    return selected_tables

def display_table_schemas():
    """
    Display schema and preview data for selected tables
    """
    if 'selected_tables' in st.session_state and st.session_state.selected_tables:
        st.subheader("Selected Tables:")
        st.write(", ".join(st.session_state.selected_tables))
    
        # Show schema for each selected table
        for table in st.session_state.selected_tables:
            with st.session_state.db_connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table}")
                schema = cursor.fetchall()
            
            with st.expander(f"{table} Schema", expanded=False):
                schema_df = pd.DataFrame(schema, columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
                st.table(schema_df)
            
            # Show a preview of the data
            with st.session_state.db_connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table} LIMIT 5")
                data = cursor.fetchall()
                if data:
                    col_names = [desc[0] for desc in cursor.description]
                    with st.expander(f"{table} Data Preview", expanded=False):
                        preview_df = pd.DataFrame(data, columns=col_names)
                        st.dataframe(preview_df)

# --- Query Processing Functions ---

def process_query(user_query, tables_selctecd, include_relationships=True):
    """
    Process the user's natural language query
    """
    # Add context about selected tables to the query
    if 'selected_tables' in st.session_state and st.session_state.selected_tables:
        context_query = f"Working with tables: {', '.join(st.session_state.selected_tables)}. {user_query}"
    else:
        context_query = user_query
    
    with st.spinner("Processing your query..."):
        try:
            # Run the pipeline with the enhanced query
            result = test_database_pipeline(query=context_query, include_tables=tables_selctecd),                             
            # After running the pipeline
            try:
                with open("data/chunk/response.txt", "r") as f:
                    response_text = f.read()
                return response_text
            except Exception as file_error:
                st.error(f"Could not read response file: {str(file_error)}")
                return None
        except Exception as e:
            st.error(f"Error processing query: {str(e)}")
            return None

def display_response(response_text):
    """
    Display the response in a structured way
    """
    if not response_text:
        return
        
    st.subheader("SQL Query Assistant Response")
    
    # If the response contains SQL code, extract and highlight it
    if "```sql" in response_text:
        parts = response_text.split("```sql")
        if len(parts) > 1:
            explanation = parts[0]
            sql_code = parts[1].split("```")[0].strip()
            
            # Show explanation
            st.write(explanation)
            
            # Show SQL code in a highlighted code block
            st.code(sql_code, language="sql")
            
            # Add SQL execution button
            if st.button("Execute SQL Query"):
                execute_sql(sql_code)
            
            # Show any additional explanation
            if len(parts[1].split("```")) > 1:
                st.write(parts[1].split("```")[1])
        else:
            st.write(response_text)
    else:
        st.write(response_text)

def execute_sql(sql_code):
    """
    Execute the generated SQL query and display results
    """
    try:
        with st.spinner("Executing query..."):
            # Execute the query
            with st.session_state.db_connection.cursor() as cursor:
                cursor.execute(sql_code)
                
                # Check if this is a SELECT query
                if sql_code.strip().upper().startswith("SELECT"):
                    # Fetch and display results
                    results = cursor.fetchall()
                    if results:
                        col_names = [desc[0] for desc in cursor.description]
                        results_df = pd.DataFrame(results, columns=col_names)
                        st.subheader("Query Results")
                        st.dataframe(results_df)
                        st.success(f"Query executed successfully. {len(results)} rows returned.")
                        
                        # Download option
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            "Download results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
                    else:
                        st.info("Query executed successfully, but no results were returned.")
                else:
                    # For non-SELECT queries (INSERT, UPDATE, DELETE)
                    rowcount = cursor.rowcount
                    st.success(f"Query executed successfully. {rowcount} rows affected.")
                    
                    # Commit changes for non-SELECT queries
                    st.session_state.db_connection.commit()
    except Exception as exec_error:
        st.error(f"Error executing query: {str(exec_error)}")
        st.session_state.db_connection.rollback()  # Rollback on error

# --- UI Layout Functions ---

def build_sidebar():
    """
    Build the sidebar with authentication and database controls
    """
    st.sidebar.header("Database Connection")

    # User authentication
    user_role = st.sidebar.text_input("Username/Role")
    password = st.sidebar.text_input("Password", type="password")
    host = st.sidebar.text_input("Host", value="localhost")
    port = st.sidebar.text_input("Port", value="3306")

    # Connect button
    connect_button = st.sidebar.button("Connect to MySQL")
    if connect_button:
        if not user_role or not password:
            st.sidebar.error("Please provide username and password")
        else:
            authenticate_mysql(host, user_role, password, port)

    # Database selection (only if authenticated)
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        selected_db = st.sidebar.selectbox("Select Database:", st.session_state.databases)
        
        if st.sidebar.button("Use Selected Database"):
            connect_to_database(host, user_role, password, port, selected_db)
        
        # Add model training section after successful database connection
        if 'current_db' in st.session_state:
            st.sidebar.header("Model Training")
            if st.sidebar.button("Train Model"):
                train_model(password, st.session_state.current_db, host, user_role)
    
    return user_role, password, host, port

def build_main_content():
    """
    Build the main content area based on state
    """
    if 'authenticated' in st.session_state and st.session_state.authenticated and 'current_db' in st.session_state:
        if 'tables' in st.session_state:
            # Table selection section
            st.header("Database Tables 💻")
            tables_selctecd = display_table_selection()
            
            # Query section
            st.header("Query Assistant")
            display_table_schemas()
            
            # Query input
            user_query = st.text_area(
                "Enter your query about the database or ask for SQL help:",
                height=150
            )
            
            # Options
            include_relationships = st.checkbox("Detect and include table relationships", value=True)
            
            # Submit button
            if st.button("Submit Query"):
                if user_query:
                    response = process_query(user_query, tables_selctecd, include_relationships)
                    display_response(response)
                else:
                    st.warning("Please enter a query before submitting.")

# --- Main App ---

def main():
    st.title("SQL Query Assistant")
    
    # Build sidebar and get authentication info
    user_role, password, host, port = build_sidebar()
    
    # Build main content based on authentication state
    build_main_content()

if __name__ == "__main__":
    main()
