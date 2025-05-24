import streamlit as st
import pymysql
from pipelines.testing_pipeline import test_database_pipeline
import io
import sys
from contextlib import redirect_stdout
from pipelines.training_pipeline import train_database_pipeline
from datetime import datetime
import pandas as pd

# Create sidebar for authentication and database selection
st.sidebar.header("Database Connection")

# User authentication
user_role = st.sidebar.text_input("Username/Role")
password = st.sidebar.text_input("Password", type="password")
host = st.sidebar.text_input("Host", value="localhost")

# Database name input
port = st.sidebar.text_input("Port", value="3306")

# Connect button
connect_button = st.sidebar.button("Connect to MySQL")

if connect_button:
    if not user_role or not password:
        st.sidebar.error("Please provide username and password")
    else:
        try:
            # Connect to MySQL server (not a specific database yet)
            connection = pymysql.connect(
                host=host,
                user=user_role,
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

# Database selection (only if authenticated)
if 'authenticated' in st.session_state and st.session_state.authenticated:
    selected_db = st.sidebar.selectbox("Select Database:", st.session_state.databases)
    
    if st.sidebar.button("Use Selected Database"):
        try:
            # Close previous connection
            if 'db_connection' in st.session_state:
                st.session_state.db_connection.close()
            
            # Clear previous database information
            if 'current_db' in st.session_state:
                # Only clear if changing to a different database
                if st.session_state.current_db != selected_db:
                    # Clean up previous database information
                    if 'tables' in st.session_state:
                        del st.session_state.tables
                    if 'selected_tables' in st.session_state:
                        del st.session_state.selected_tables
            
            # Connect to the selected database
            db_connection = pymysql.connect(
                host=host,
                user=user_role,
                password=password,
                port=int(port),
                database=selected_db
            )
            
            # Store the database connection
            st.session_state.db_connection = db_connection
            st.session_state.current_db = selected_db
            
            st.sidebar.success(f"Connected to database: {selected_db}")
            # st.experimental_rerun()  # Rerun to clear any displayed information from previous DB
            
        except Exception as e:
            st.sidebar.error(f"Failed to connect to database: {str(e)}")
    
    # Add model training section after successful database connection
    if 'current_db' in st.session_state:
        st.sidebar.header("Model Training")
        if st.sidebar.button("Train Model"):
            with st.spinner("Training model with database data..."):
                try:
                    # Call the training pipeline with the connected database parameters
                    train_database_pipeline(
                        password=password,
                        database_name=st.session_state.current_db,
                        host=host,
                        user=user_role
                    )
                    st.sidebar.success("Model training completed successfully!")
                    
                    # Get table names after successful model training
                    with st.session_state.db_connection.cursor() as cursor:
                        cursor.execute("SHOW TABLES")
                        st.session_state.tables = [table[0] for table in cursor.fetchall()]
                    
                except Exception as e:
                    st.sidebar.error(f"Model training failed: {str(e)}")

        # Display table selection if tables exist in session state
        if 'tables' in st.session_state:
            st.header("Database Tables")
            
            # Use multiselect instead of buttons for table selection
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
            
            # Query input section
            st.header("Query Assistant")
            
            # Display all selected tables
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
            
            # Text area for entering SQL query
            user_query = st.text_area(
                "Enter your query about the database or ask for SQL help:",
                height=150
            )
            
            # Checkbox for including table relationships
            include_relationships = st.checkbox("Detect and include table relationships", value=True)
            
            # Submit button
            if st.button("Submit Query"):
                if user_query:
                    # Add context about selected tables to the query
                    if 'selected_tables' in st.session_state and st.session_state.selected_tables:
                        context_query = f"Working with tables: {', '.join(st.session_state.selected_tables)}. {user_query}"
                    else:
                        context_query = user_query
                        
                    with st.spinner("Processing your query..."):
                        try:
                            # Run the pipeline with the enhanced query
                            result = test_database_pipeline(query=context_query)                            
                            # After running the pipeline
                            try:
                                with open("data/chunk/response.txt", "r") as f:
                                    response_text = f.read()
                                    
                                # Display the response in a more structured way
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
                                        
                                        # Show any additional explanation
                                        if len(parts[1].split("```")) > 1:
                                            st.write(parts[1].split("```")[1])
                                    else:
                                        st.write(response_text)
                                else:
                                    st.write(response_text)
                            except Exception as file_error:
                                st.error(f"Could not read response file: {str(file_error)}")
                        except Exception as e:
                            st.error(f"Error processing query: {str(e)}")
                else:
                    st.warning("Please enter a query before submitting.")
