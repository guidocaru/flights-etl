from dags.transform.transform import transform_data_mock
import awswrangler as wr
import redshift_connector
from config.config import ENV
from pandas import DataFrame


def load_to_redshift(dataframes: list[DataFrame]):
    """
    Loads a set of pandas dataframes into the corresponding tables in an Amazon Redshift database
    """

    conn_params = {
        "host": ENV["REDSHIFT_HOST"],
        "port": ENV["REDSHIFT_PORT"],
        "database": ENV["REDSHIFT_DB"],
        "user": ENV["REDSHIFT_USER"],
        "password": ENV["REDSHIFT_PASSWORD"],
    }

    try:
        conn = redshift_connector.connect(**conn_params)
    except redshift_connector.Error as e:
        raise ConnectionError("Error connecting to Redshift: ", e)

    try:
        for table_name, dataframe in dataframes.items():
            wr.redshift.to_sql(
                table=table_name,
                df=dataframe,
                con=conn,
                schema=ENV["REDSHIFT_SCHEMA"],
                mode="overwrite",
                use_column_names=True,
                lock=True,
                index=False,
            )
    except Exception as e:
        raise Exception(f"Error loading data to Redshift: {str(e)}")
    finally:
        if conn:
            conn.close()


dataframes = transform_data_mock("dags/extract/mock_flights.json")

load_to_redshift(dataframes)
