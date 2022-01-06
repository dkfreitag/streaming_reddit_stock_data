import os
import argparse
import time
import datetime as dt

import pytz
import praw
import pandas as pd

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2

# If you update the reddit_submission.proto protocol buffer definition, run:
#
#   protoc --python_out=. reddit_submission.proto
#
# to generate the reddit_submission_pb2.py module.

import reddit_submission_pb2

from dotenv import load_dotenv
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# handles parsing the arguments the user provides
def parse_arguments():
    parser = argparse.ArgumentParser(description='Stream submissions from reddit to BigQuery table.')
    parser.add_argument('--subreddit', type=str, required=True, help='Required. Name of subreddit to stream.')
    parser.add_argument('--project', type=str, required=True, help='Required. Name of the GCP project.')
    parser.add_argument('--dataset', type=str, required=True, help='Required. Name of the dataset.')
    parser.add_argument('--table', type=str, required=True, help='Required. Name of the table to store data.')
    
    args = parser.parse_args()
    return args.subreddit, args.project, args.dataset, args.table
    
# set up the connection
def create_reddit_conn(client_id, client_secret):
	reddit_obj = praw.Reddit(client_id = client_id,
	                     client_secret = client_secret,
	                     user_agent = "Post and comment extraction")
	print("Reddit connection established.")
	return reddit_obj

# function to create a row of data that can be fed to the stream
def create_row_data(id: str, title: str, selftext: str, created_utc: str, streamed_utc: str):
    row = reddit_submission_pb2.RedditSubmission()
    row.id = id
    row.title = title
    row.selftext = selftext
    row.created_utc = created_utc
    row.streamed_utc = streamed_utc
    return row.SerializeToString()

# function to append rows in committed mode to the stream
def append_rows_committed(project_id: str, dataset_id: str, table_id: str, reddit_obj, subreddit_name):

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
    reddit_submission_pb2.RedditSubmission.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests. Construct an
    # AppendRowsStream to send an arbitrary number of requests to a stream.
    append_rows_stream = writer.AppendRowsStream(write_client, request_template)

    for submission in reddit_obj.subreddit(subreddit_name).stream.submissions():
        # Create a batch of row data by appending proto2 serialized bytes to the
        # serialized_rows repeated field.
        proto_rows = types.ProtoRows()
        
        proto_rows.serialized_rows.append(create_row_data(submission.id, submission.title, submission.selftext, dt.datetime.fromtimestamp(submission.created_utc, tz=dt.timezone.utc).strftime("%Y-%m-%d %T.%f"), dt.datetime.fromtimestamp(time.time(), tz=dt.timezone.utc).strftime("%Y-%m-%d %T.%f")))

        request = types.AppendRowsRequest()
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows
        request.proto_rows = proto_data

        response_future_1 = append_rows_stream.send(request)

        print(response_future_1.result())

    # Shutdown background threads and close the streaming connection.
    # We should only get here if there is an error.
    append_rows_stream.close()

    print(f"Writes to stream: '{write_stream.name}' have been committed.")
    print("There must have been an error for us to end up here though...")

def main():
    try:
        # parse arguments
        subreddit_name, project_name, dataset_name, table_name = parse_arguments()
        
        # create reddit connection
        r_obj = create_reddit_conn(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        
        # start the stream
        append_rows_committed(project_id=project_name, dataset_id=dataset_name, table_id=table_name, reddit_obj=r_obj, subreddit_name=subreddit_name)
        
    # if an exception occurs when executing the above code, try again
    except Exception as e:
        print("Exception occurred:", e)
        print("Restarting.")
        main()
    
if __name__ == '__main__':
    main()
