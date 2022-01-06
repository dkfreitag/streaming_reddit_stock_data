## Stream Reddit Posts, Comments, and Stock Ticker Data into BigQuery

Python scripts to capture streaming reddit comments, streaming reddit posts, or streaming stock data and store it in BigQuery.

#### Build docker container:

`docker build -t reddit_stream_capture:1.0 .`

#### Running a script from docker:

```
docker run -t \
-e CLIENT_ID=client_id_goes_here \
-e CLIENT_SECRET=client_secret_goes_here \
-e GOOGLE_APPLICATION_CREDENTIALS=google_app_cred_file.json \
reddit_stream_capture:1.0 \
src/stream_comments.py \
--subreddit subreddit_name \
--project project_name \
--dataset dataset_name \
--table table_name
```

#### Scripts in `src/` that can be run:

* `stream_comments.py`
* `stream_submissions.py`
* `stream_stock_data.py`

#### Authentication Needed

* Reddit API key - [get one here](https://ssl.reddit.com/prefs/apps/)
* Google Cloud account
	* Service account with a private JSON key - [instructions] (https://cloud.google.com/docs/authentication/getting-started)

#### Documentation Referenced

PRAW Documentation: [https://praw.readthedocs.io/en/stable/index.html](https://praw.readthedocs.io/en/stable/index.html)

Reddit API Key Signup for Application Developers: [https://ssl.reddit.com/prefs/apps/](https://ssl.reddit.com/prefs/apps/)

yfinance Documentation: [https://github.com/ranaroussi/yfinance](https://github.com/ranaroussi/yfinance)

BigQuery Storage Write API: [https://cloud.google.com/bigquery/docs/write-api](https://cloud.google.com/bigquery/docs/write-api)