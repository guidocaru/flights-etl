from dags.transform.transform import transform_data_mock
from config.config import ENV
import awswrangler as wr
import redshift_connector


def load_to_redshift(dataframes):

    conn_params = {
        "host": ENV["REDSHIFT_HOST"],
        "port": ENV["REDSHIFT_PORT"],
        "database": ENV["REDSHIFT_DB"],
        "user": ENV["REDSHIFT_USER"],
        "password": ENV["REDSHIFT_PASSWORD"],
    }

    conn = redshift_connector.connect(**conn_params)

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


dataframes = transform_data_mock("dags/extract/mock_flights.json")

load_to_redshift(dataframes)
