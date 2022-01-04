import os
import argparse
import time
import datetime as dt

import pytz
import pandas as pd
import yfinance as yf

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2

# If you update the reddit_comment.proto protocol buffer definition, run:
#
#   protoc --python_out=. reddit_comment.proto
#
# to generate the reddit_comment_pb2.py module.

import stock_price_pb2

from dotenv import load_dotenv
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# handles parsing the arguments the user provides
def parse_arguments():
    parser = argparse.ArgumentParser(description='Stream stock data from Yahoo! Finance to BigQuery table.')
    parser.add_argument('--ticker', type=str, required=True, help='Required. Name of ticker to stream.')
    parser.add_argument('--project', type=str, required=True, help='Required. Name of the GCP project.')
    parser.add_argument('--dataset', type=str, required=True, help='Required. Name of the dataset.')
    parser.add_argument('--table', type=str, required=True, help='Required. Name of the table to store data.')
    parser.add_argument('--market_hours_only', help='If this flag is given, data is only streamed during market hours.', action='store_true')
    parser.add_argument('--data_frequency', type=int, default=30, help='The number of seconds between data pulls, i.e. time to sleep after each data point. Default = 30 seconds.')
    
    args = parser.parse_args()
    return args.ticker, args.project, args.dataset, args.table, args.market_hours_only, args.data_frequency
   

# function to create a row of data that can be fed to the stream
def create_row_data(ticker: str,
                    market_time_utc: str,
                    open_price: float,
                    high_price: float,
                    low_price: float,
                    close_price: float,
                    adj_close: float,
                    volume: int,
                    streamed_utc: str):
    row = stock_price_pb2.StockPrice()
    row.ticker = ticker
    row.market_time_utc = market_time_utc
    row.open_price = open_price
    row.high_price = high_price
    row.low_price = low_price
    row.close_price = close_price
    row.adj_close = adj_close
    row.volume = volume
    row.streamed_utc = streamed_utc
    
    return row.SerializeToString()

# function to append rows in committed mode to the stream
def append_rows_committed(project_id: str, dataset_id: str, table_id: str, ticker: str, market_hours_only, seconds_to_wait):

    """Create a write stream, write the data, close the stream if we exit the loop because the generator stops."""
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    write_stream = types.WriteStream()

    # Create write stream and use COMMITTED for the type so it writes immediately
    write_stream.type_ = types.WriteStream.Type.COMMITTED
    write_stream = write_client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name
    
    print('Created stream:', stream_name)

    # Create a template with fields needed for the first request.
    request_template = types.AppendRowsRequest()

    # The initial request must contain the stream name.
    request_template.write_stream = stream_name

    # So that BigQuery knows how to parse the serialized_rows, generate a
    # protocol buffer representation of your message descriptor.
    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    stock_price_pb2.StockPrice.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests. Construct an
    # AppendRowsStream to send an arbitrary number of requests to a stream.
    append_rows_stream = writer.AppendRowsStream(write_client, request_template)

    if market_hours_only == True:
	    while True:
	        current_time = dt.datetime.fromtimestamp(time.time(), tz=pytz.timezone('US/Eastern')).time()
	        current_weekday = dt.datetime.fromtimestamp(time.time(), tz=pytz.timezone('US/Eastern')).weekday()
	        market_open = dt.datetime.strptime('09:30:00', '%H:%M:%S').time()
	        market_close = dt.datetime.strptime('16:00:00', '%H:%M:%S').time()
	        
	        # if it is between 9:30 AM and 4:00 PM Eastern and it is not a weekend, markets are open
	        if ((current_time >= market_open) and (current_time <= market_close) and (current_weekday < 5)):
	            # Markets are open
	            data = yf.download(tickers=ticker, period='1d', interval='1m')
	            
	            # Create a batch of row data by appending proto2 serialized bytes to the
	            # serialized_rows repeated field.
	            proto_rows = types.ProtoRows()

	            proto_rows.serialized_rows.append(create_row_data(ticker,
	                                                                str(pd.Timestamp(data.tail(1).index.values[0])),
	                                                                data.tail(1)['Open'].values[0],
	                                                                data.tail(1)['High'].values[0],
	                                                                data.tail(1)['Low'].values[0],
	                                                                data.tail(1)['Close'].values[0],
	                                                                data.tail(1)['Adj Close'].values[0],
	                                                                data.tail(1)['Volume'].values[0],
	                                                                dt.datetime.fromtimestamp(time.time(), tz=dt.timezone.utc).strftime("%Y-%m-%d %T.%f")
	                                                               )
	                                             )
	            
	            request = types.AppendRowsRequest()
	            proto_data = types.AppendRowsRequest.ProtoData()
	            proto_data.rows = proto_rows
	            request.proto_rows = proto_data
	
	            response_future_1 = append_rows_stream.send(request)
	
	            print(response_future_1.result())
	            
	        else:
	            # markets are closed
	            pass
	
	        # sleep for a specified number of seconds before retrieving data again
	        time.sleep(seconds_to_wait)
    else:
        while True:
            # get market data
            data = yf.download(tickers=ticker, period='1d', interval='1m')
            
            # Create a batch of row data by appending proto2 serialized bytes to the
            # serialized_rows repeated field.
            proto_rows = types.ProtoRows()

            proto_rows.serialized_rows.append(create_row_data(ticker,
                                                                str(pd.Timestamp(data.tail(1).index.values[0])),
                                                                data.tail(1)['Open'].values[0],
                                                                data.tail(1)['High'].values[0],
                                                                data.tail(1)['Low'].values[0],
                                                                data.tail(1)['Close'].values[0],
                                                                data.tail(1)['Adj Close'].values[0],
                                                                data.tail(1)['Volume'].values[0],
                                                                dt.datetime.fromtimestamp(time.time(), tz=dt.timezone.utc).strftime("%Y-%m-%d %T.%f")
                                                               )
                                             )
            
            request = types.AppendRowsRequest()
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = proto_rows
            request.proto_rows = proto_data

            response_future_1 = append_rows_stream.send(request)

            print(response_future_1.result())
            
            # sleep for a specified number of seconds before retrieving data again
            time.sleep(seconds_to_wait)
           

    # Shutdown background threads and close the streaming connection.
    # We should only get here if there is an error.
    append_rows_stream.close()

    print(f"Writes to stream: '{write_stream.name}' have been committed.")
    print("There must have been an error for us to end up here though...")

def main():
    # parse arguments
    ticker_name, project_name, dataset_name, table_name, m_hours, data_frequency = parse_arguments()
    
    # start the stream
    append_rows_committed(project_id=project_name, dataset_id=dataset_name, table_id=table_name, ticker=ticker_name, market_hours_only=m_hours, seconds_to_wait=data_frequency)
    
if __name__ == '__main__':
    main()
