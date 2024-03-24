from scheduled_jobs.data_providers.coinmarketcap import CoinMarketCap
from scheduled_jobs.utils import helpers
import pandas as pd
from datetime import datetime as dt


class RefData(CoinMarketCap):
    schema = "public"

    def __init__(self):
        helpers.LOG.info("Launching reference data job")
        super().__init__()
        helpers.LOG.info("Connecting to C-Metrics database...")
        self.db = helpers.get_db_connection(local=False)
        helpers.LOG.info("Connected to C-Metrics database")
        self.mapping = pd.DataFrame()

    def update_table(self, df: pd.DataFrame, table: str):
        query = f"DELETE FROM {self.schema}.{table}"
        helpers.execute_query(self.db, query)
        helpers.LOG.info(f"Cleared {table}")
        df["insert_tmstmp"] = dt.now()
        df.to_sql(
            name=table, schema=self.schema, con=self.db, if_exists="append", index=False
        )
        helpers.LOG.info(f"Added {len(df)} records to {table}")

    def update_mapping(self):
        helpers.LOG.info("Retrieving CoinMarketCap mapping")
        mapping = self.get_endpoint(
            api_version=1, category="cryptocurrency", endpoint="map"
        )
        df = pd.DataFrame(mapping)
        table = "cmetrics_coinmarketcapmapping"
        self.update_table(df, table)
        self.mapping = mapping

    def get_meta_data(self):
        data = list()
        for pair in self.mapping:
            helpers.LOG.info(f"Retrieving {pair['name']} metadata")
            meta_data = self.get_endpoint(
                api_version=2,
                category="cryptocurrency",
                endpoint=f"info?id={pair['id']}",
            )
            meta_data = meta_data[str(pair["id"])]
            data.append(meta_data)
        df = pd.DataFrame(data)
        df.columns = [col.replace("-", "_") for col in df.columns]
        cols_to_flatten = ("tag", "urls", "platform", "contract_address")
        for col in df.columns:
            for flat_col in cols_to_flatten:
                if flat_col in col:
                    df[col] = df[col].astype(str)
        self.update_table(df, "cmetrics_coinmarketcapmetadata")

    def update_ref_data(self):
        self.update_mapping()
        self.get_meta_data()
