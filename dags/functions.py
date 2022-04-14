import datetime
import json
import os
import pathlib

import pandas as pd
import requests


class DataFetcher:
    ENDPOINT_TXS = "https://dummy-ll-api-gateway-j4dbqscusq-ew.a.run.app/transactions"

    def __init__(self, max_txs_per_page=1000):
        self._token = os.environ["TRX_TOKEN"]
        self._max_txs_per_page = max_txs_per_page

    def fetch(self, date: str):
        return self._fetch_all_pages(date)

    def _fetch_page(self, date: str, page_number: int):
        response = requests.request(
            "GET",
            self.ENDPOINT_TXS,
            headers={"Authorization": f"Bearer {self._token}"},
            params={
                "start_date": date,
                "max_txs": self._max_txs_per_page,
                "page": page_number,
            },
        )
        transactions = json.loads(response.json()["data"])
        data = []
        for transaction in transactions:
            trx_date = datetime.datetime.utcfromtimestamp(
                transaction["Transaction datetime"] / 1000
            ).strftime("%Y-%m-%d")
            if date == trx_date:
                data.append(transaction)
        return data

    def _fetch_all_pages(self, date):
        proceed = True
        data = []
        current_page = 0
        while proceed:
            chunk = self._fetch_page(date, current_page)
            current_page += 1
            data.extend(chunk)
            if len(chunk) == 0:
                proceed = False
        return data


def save_transactions_from_api(ds):
    fetcher = DataFetcher()
    transactions = fetcher.fetch(ds)
    df = pd.DataFrame(transactions)
    if len(df.index) == 0:
        return
    df.columns = [
        "transaction_datetime",
        "amount_from",
        "currency_from",
        "currency_to",
        "status",
    ]
    pathlib.Path("/tmp/data").mkdir(parents=True, exist_ok=True)
    df.to_csv(f"/tmp/data/transactions-{ds}.csv", index=False)


def compute_total_amount_eur_to_btc():
    data_path = pathlib.Path("/tmp/data")
    all_files = data_path.glob("*.csv")
    df_list = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df_list.append(df)
    df_all = pd.concat(df_list, axis=0, ignore_index=True)
    df_filtered = df_all[
        (df_all.currency_from == "EUR")
        & (df_all.currency_to == "BTC")
        & (df_all.status == "completed")
    ]
    print(df_filtered.amount_from.sum())
