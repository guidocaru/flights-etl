import awswrangler as wr
import redshift_connector
from typing import Any
import os


def load_to_redshift(**context: Any) -> None:
    """
    Loads a set of pandas dataframes into the corresponding tables in an Amazon Redshift database
    """

    transformed_data = context["ti"].xcom_pull(key="transformed_data")

    conn_params = {
        "host": os.environ.get("REDSHIFT_HOST"),
        "port": os.environ.get("REDSHIFT_PORT"),
        "database": os.environ.get("REDSHIFT_DB"),
        "user": os.environ.get("REDSHIFT_USER"),
        "password": os.environ.get("REDSHIFT_PASSWORD"),
    }

    try:
        conn = redshift_connector.connect(**conn_params)
    except redshift_connector.Error as e:
        raise ConnectionError("Error connecting to Redshift: ", e)

    try:
        for table_name, dataframe in transformed_data.items():
            wr.redshift.to_sql(
                table=table_name,
                df=dataframe,
                con=conn,
                schema=os.environ.get("REDSHIFT_SCHEMA"),
                mode="overwrite",
                use_column_names=True,
                lock=True,
                index=False,
            )
    except Exception as e:
        raise Exception("Error loading data to Redshift: ", e)
    finally:
        if conn:
            conn.close()
