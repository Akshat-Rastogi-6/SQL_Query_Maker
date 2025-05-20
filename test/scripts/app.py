import os
import streamlit as st
import pymysql
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv('.env')

genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',
            database='practice',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except pymysql.MySQLError as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

def fetch_tables(connection):
    try:
        if connection is None:
            st.error("No valid database connection.")
            return None
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table['Tables_in_practice'] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except pymysql.MySQLError as e:
        st.error(f"Error fetching tables: {e}")
        return None
    
def fetch_data_schema(connection, table):
    try:
        if connection is None:
            st.error("No valid database connection.")
            return None
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        data = cursor.fetchall()
        cursor.execute(f"DESCRIBE {table}")
        schema = cursor.fetchall()
        cursor.close()
        return data, schema
    except pymysql.MySQLError as e:
        st.error(f"Error fetching table data: {e}")
        return None, None

def initialize_agent(schema, user_query, table):
    model = genai.GenerativeModel(
        model_name='gemini-1.5-flash', 
        system_instruction= f"You are a simple data analysis module that takes in schema or definition of the table and a user_query from the user and returns the sql query to perform the task. The schema for the table is here : {schema}, The table name is {table}. Strictly generate only the sql query."
        "Please don't give in markdown format."
    )
    response = model.generate_content(user_query)
    return response.text

def process_generated_query(sql_query, connection, table):
    try:
        if connection is None:
            st.error("No valid database connection.")
            return None
        cursor = connection.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()
        cursor.close()
        return data
    except pymysql.MySQLError as e:
        st.error(f"Error fetching table data: {e}")
        return None
    
def analyze_the_generated_table(result, user_query):
    model = genai.GenerativeModel(
        model_name='gemini-1.5-flash', 
        system_instruction= f"You are a simple data analysis module that takes in the result of the query and the user_query from the user and returns the analysis of the data. The result of the query is here : {result}, The user query is {user_query}. Strictly generate only the analysis."
    )
    response = model.generate_content(user_query)
    return response.text
    

# Streamlit app
st.title("MySQL Table Selector")

connection = connect_to_mysql()
if connection:
    tables = fetch_tables(connection)
    if tables:
        selected_table = st.selectbox("Select a table to work with:", tables, label_visibility="collapsed")
        st.write(f"You selected: {selected_table}")
        st.write("Table data:")
        data, schema = fetch_data_schema(connection, selected_table)
        if data:
            st.table(data)
            user_query = st.text_area("Enter a custom query:")
            if st.button("Run query"):
                sql_query = initialize_agent(schema, user_query, selected_table)
                st.write("SQL Query:")
                st.code(sql_query)
                result = process_generated_query(sql_query, connection, selected_table)
                if result:
                    st.table(result)
                    analysis = analyze_the_generated_table(result, user_query)
                    st.write("Analysis:")
                    st.write(analysis)
                else:
                    st.error("Failed to fetch table data.")                            
        else:
            st.error("Failed to fetch table data.")

    connection.close()
else:
    st.error("Failed to connect to the database.")
