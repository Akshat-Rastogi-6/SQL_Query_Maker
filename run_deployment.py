import streamlit as st
import pymysql
from pipelines.testing_pipeline import test_database_pipeline
from pipelines.training_pipeline import train_database_pipeline
from src.data_response import GeminiResponse
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
            result = test_database_pipeline(query=context_query, include_tables=tables_selctecd)                             
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
    
    # Save response text to session state so it persists across reruns
    st.session_state.current_response_text = response_text
        
    st.subheader("SQL Query Assistant Response")
    
    # If the response contains SQL code, extract and highlight it
    if "```sql" in response_text:
        parts = response_text.split("```sql")
        if len(parts) > 1:
            explanation = parts[0]
            sql_code = parts[1].split("```")[0].strip()
            
            # Save SQL code to session state
            st.session_state.current_sql_code = sql_code
            
            # Show explanation
            st.write(explanation)
            
            # Show SQL code in a highlighted code block
            st.code(sql_code, language="sql")
            
            # Add SQL execution button with a unique key
            if st.button("Execute SQL Query", key="execute_sql_button") or st.session_state.get('sql_executed', False):
                # Save flag that we've executed SQL (persists across reruns)
                st.session_state.sql_executed = True
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
        # First check if we already have results in session state to avoid re-executing
        if 'sql_results' not in st.session_state or st.session_state.get('current_sql') != sql_code:
            with st.spinner("Executing query..."):
                # Check for empty or dangerous queries
                if not sql_code or "DROP " in sql_code.upper() and not st.session_state.get("allow_dangerous", False):
                    st.warning("Potentially dangerous query detected. Add a safety confirmation checkbox if you really need this.")
                    return
                    
                # Execute the query
                with st.session_state.db_connection.cursor() as cursor:
                    cursor.execute(sql_code)
                    
                    # Better detection of query type
                    query_type = sql_code.strip().upper()
                    is_select = query_type.startswith("SELECT") or "SELECT" in query_type and not any(
                        word in query_type for word in ["UPDATE", "INSERT", "DELETE", "DROP", "CREATE", "ALTER"]
                    )
                    
                    # Store the execution state
                    st.session_state.current_sql = sql_code
                    
                    if is_select:
                        # Fetch results
                        results = cursor.fetchall()
                        if results:
                            col_names = [desc[0] for desc in cursor.description]
                            results_df = pd.DataFrame(results, columns=col_names)
                            
                            # Store in session state
                            st.session_state.sql_results = {
                                "is_select": True,
                                "dataframe": results_df,
                                "sql": sql_code,
                                "row_count": len(results),
                                "col_names": col_names
                            }
                        else:
                            st.session_state.sql_results = {
                                "is_select": True,
                                "dataframe": None,
                                "sql": sql_code,
                                "row_count": 0
                            }
                    else:
                        # For non-SELECT queries
                        rowcount = cursor.rowcount
                        st.session_state.db_connection.commit()
                        st.session_state.sql_results = {
                            "is_select": False,
                            "dataframe": None,
                            "sql": sql_code,
                            "row_count": rowcount,
                            "type": "modification"
                        }
        
        # Now display the results (either fresh or from session state)
        results_data = st.session_state.sql_results
        
        if results_data["is_select"]:
            if results_data.get("dataframe") is not None:
                results_df = results_data["dataframe"]
                
                # Display results in an expander
                with st.expander("Query Results", expanded=True):
                    st.success(f"Query executed successfully. {results_data['row_count']} rows returned.")
                    
                    # Handle large result sets
                    if results_data['row_count'] > 1000:
                        st.warning(f"Large result set ({results_data['row_count']} rows). Showing first 1000 rows.")
                        st.dataframe(results_df.head(1000))
                    else:
                        st.dataframe(results_df)
                
            else:
                st.info("Query executed successfully, but no results were returned.")
        else:
            # For non-SELECT queries (INSERT, UPDATE, DELETE)
            with st.expander("Query Results", expanded=True):
                st.success(f"Query executed successfully. {results_data['row_count']} rows affected.")
                
    except Exception as exec_error:
        st.error(f"Error executing query: {str(exec_error)}")
        
        # Show detailed error information
        if "access denied" in str(exec_error).lower():
            st.error("You don't have permission to run this query. Contact your database administrator.")
        elif "syntax error" in str(exec_error).lower():
            st.error("The query contains a syntax error. Please check your SQL syntax.")
            
        # Rollback on error
        try:
            st.session_state.db_connection.rollback()
        except:
            st.error("Additionally, there was an issue rolling back the transaction.")

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
            tables_selected = display_table_selection()
            
            # Query section
            st.header("Query Assistant")
            display_table_schemas()
            
            # Query input
            user_query = st.text_area(
                "Enter your query about the database or ask for SQL help:",
                height=150,
                key="query_input"
            )
            
            # Options
            include_relationships = st.checkbox("Detect and include table relationships", value=True)
            
            # Submit button
            if st.button("Submit Query", key="submit_query_button"):
                if user_query:
                    response = process_query(user_query, tables_selected, include_relationships)
                    display_response(response)
                else:
                    st.warning("Please enter a query before submitting.")
            
            # Important: Check if there's a previous response to display
            elif 'current_response_text' in st.session_state:
                # This ensures the response stays visible after executing SQL
                display_response(st.session_state.current_response_text)

# --- Main App ---

def main():
    st.title("SQL Query Assistant")
    
    # Build sidebar and get authentication info
    user_role, password, host, port = build_sidebar()
    
    # Build main content based on authentication state
    build_main_content()

if __name__ == "__main__":
    main()
