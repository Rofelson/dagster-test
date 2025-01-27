import polars as pl
import dagster as dg
from dagster import EnvVar


@dg.asset
def imported_data():
    ## Read data from the database
    query = "select * from contacts"
    database_name = EnvVar("DATABASE_SCHEMA").get_value()
    database_username = EnvVar("DATABASE_USERNAME").get_value()
    database_password = EnvVar("DATABASE_PASSWORD").get_value()
    database_port = EnvVar("DATABASE_PORT").get_value()
    database_server = EnvVar("DATABASE_SERVER").get_value()
    uri = f"mysql://{database_username}:{database_password}@{database_server}:{database_port}/{database_name}"
    df = pl.read_database_uri(query, uri, engine="connectorx")
    df.write_parquet("data/contacts.parquet")
    return "Data loaded succesfully"


## Tell Dagster about the assets that make up the pipeline
## by passing it to the Definitions object
## This allows Dagster to manage the assets' execution and dependencies
defs = dg.Definitions(assets=[imported_data])
