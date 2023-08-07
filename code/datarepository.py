

from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx4g"])
s.start()

import requests
from deephaven.time import to_datetime
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven import agg
from websocket import create_connection, WebSocketConnectionClosedException
from deephaven import pandas as dhpd

from threading import Thread
import json

class BTCfetcher():
    def __init__(self) -> None:
        self.base_api_url = "wss://ws-feed.exchange.coinbase.com"
        self.ws = create_connection(self.base_api_url)
    def subscribe(self):
        self.ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": ["BTC-USD"],
                            "channels": ["matches"],
                        }
                    )
                )
    def coinbase_time_to_datetime(strn):
        return to_datetime(strn[0:-1] + " UTC")
    def dtw_table(self):
        dtw_column_converter = {
        'size': float,
        'price': float,
        'time': self.coinbase_time_to_datetime
    }
        self.dtw_columns = {
            'product_id': dht.string,
            'time': dht.DateTime,
            'side': dht.string,
            'size': dht.float_,
            'price': dht.float_,
            'type': dht.string,
            'trade_id': dht.int_,
            'maker_order_id': dht.string,
            'taker_order_id': dht.string,
            'sequence': dht.int_,
        }

        self.dtw = DynamicTableWriter(self.dtw_columns)
    def thread_function(self):
        while True:
            try:
                data = json.loads(self.ws.recv())
                row_to_write = []
                for key in self.dtw_columns:
                    value = None
                    if key in self.dtw_column_converter:
                        value = self.dtw_column_converter[key](data[key])
                    else:
                        value = data[key]
                    row_to_write.append(value)

                self.dtw.write_row(*row_to_write)
            except Exception as e:
                print(e)
    def start_collection(self):
        thread = Thread(target=self.thread_function)
        thread.start()
        coinbase_websocket_table = self.dtw.table
        agg_list = [
        agg.avg(cols=["avg_price = price"]),
        agg.count_("trade_count")]
        summary_10s = coinbase_websocket_table.update(["time_10s = lowerBin(time, SECOND * 10)"]).agg_by(agg_list, by=["time_10s"])
        self.data_frame = dhpd.to_pandas(summary_10s)

    def run(self):
        self.subscribe()
        self.dtw_table()
        self.start_collection()
        return self.data_frame





