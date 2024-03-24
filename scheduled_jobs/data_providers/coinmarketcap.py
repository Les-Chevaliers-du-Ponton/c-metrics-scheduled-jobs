import json
import time

from dotenv import load_dotenv
from requests import Session
import os

from scheduled_jobs.utils.helpers import LOG

load_dotenv()


class CoinMarketCap:

    def __init__(self):
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": os.getenv("COINMARKETCAP_API_KEY"),
        }
        self.session = Session()
        self.session.headers.update(headers)
        self.throtle_seconds = 60

    def format_payload(self, data: list) -> list:
        formatted_data = list()
        is_flat = False if isinstance(data, list) else True
        data = [data] if is_flat else data
        for record in data:
            for k, v in record.items():
                if isinstance(v, list) or isinstance(v, dict) and not is_flat:
                    record[k] = str(v)
            formatted_data.append(record)
        return formatted_data[0] if is_flat else formatted_data

    def handle_error(self, error: str):
        LOG.error(
            f"An error occurred : {error}\nRetrying in {self.throtle_seconds} seconds..."
        )
        time.sleep(self.throtle_seconds)

    def get_endpoint(self, api_version: int, category: str, endpoint: str) -> dict:
        url = f"{os.getenv('COINMARKETCAP_HOST')}/v{api_version}/{category}/{endpoint}"
        while True:
            try:
                response = self.session.get(url)
                data = json.loads(response.text)
                error = data["status"]["error_message"]
                if error:
                    self.handle_error(error)
                else:
                    data = self.format_payload(data["data"])
                    return data
            except Exception as error:
                self.handle_error(error)
