import psycopg2
from psycopg2 import sql
from config.config import ENV


class Redshift:
    def __init__(self):
        """Initialize RedshiftDB with connection parameters."""
        self.host = ENV["REDSHIFT_HOST"]
        self.port = ENV["REDSHIFT_PORT"]
        self.dbname = ENV["REDSHIFT_DB"]
        self.user = ENV["REDSHIFT_USER"]
        self.password = ENV["REDSHIFT_PASSWORD"]
        self.connection = None

    def connect(self):
        """Establish a connection to the Redshift database."""

        if not self.connection:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                )
                print("Connection to Redshift established.")
            except Exception as e:
                print(f"Error connecting to Redshift: {e}")
                raise

    def disconnect(self):
        """Close the connection to the Redshift database."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                print("Disconnected from Redshift.")
            except Exception as e:
                print(f"Error disconnecting from Redshift: {e}")

    def insert_data(self, table, data):
        """Insert data into the specified table."""
        keys = data.keys()
        values = data.values()

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, keys)),
            values=sql.SQL(", ").join(sql.Placeholder() * len(values)),
        )

        try:
            self.execute_query(query, params=tuple(values))
            print(f"Data inserted into {table}.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            raise

    def update_data(self, table, data, condition):
        """Update data in the specified table based on a condition."""
        set_clause = ", ".join(f"{k} = %s" for k in data.keys())
        where_clause = " AND ".join(f"{k} = %s" for k in condition.keys())

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(condition.values())

        try:
            self.execute_query(query, params=params)
            print(f"Data updated in {table}.")
        except Exception as e:
            print(f"Error updating data: {e}")
            raise
