import logging
import pymysql

class IngestData:
    def __init__(self, password:str, database_name:str, host:str, user:str):
        self.password = password
        self.database_name = database_name
        self.host = host
        self.user = user
    
    def connect_to_database(self) -> pymysql.connections.Connection:
        """Establishes a connection to the MySQL database.
        Returns:
            pymysql.connections.Connection: The connection object to the database.
        """
        try:
            connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database_name,
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except pymysql.MySQLError as e:
            logging.error(f"Error connecting to MySQL: {e}")
            return None
        
    def fetch_tables(self, connection: pymysql.connections.Connection) -> list:
        """Fetches the list of tables in the database.
        Args:
            connection (pymysql.connections.Connection): The database connection object.
        Returns:
            list: A list of table names in the database.
        """
        try:
            if connection is None:
                logging.error("No valid database connection.")
                return None
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table['Tables_in_practice'] for table in cursor.fetchall()]
            cursor.close()
            return tables
        except pymysql.MySQLError as e:
            logging.error(f"Error fetching tables: {e}")
            return None
        
    def fetch_table_schemas(self, connection: pymysql.connections.Connection, table: str) -> list:
        """Fetches the schema of a specified table.
        Args:
            connection (pymysql.connections.Connection): The database connection object.
            table (str): The name of the table to fetch the schema for.
        Returns:
            list: A list of tuples representing the schema of the table.
        """
        try:
            if connection is None:
                logging.error("No valid database connection.")
                return None
            cursor = connection.cursor()
            cursor.execute(f"DESCRIBE {table}")
            schema = cursor.fetchall()
            cursor.close()
            return schema
        except pymysql.MySQLError as e:
            logging.error(f"Error fetching table data: {e}")
            return None