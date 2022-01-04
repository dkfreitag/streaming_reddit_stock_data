until python3 src/stream_stock_data.py --ticker BTC-USD --project reddit-historical-336415 --dataset reddit_historical --table 01streaming_stock_prices_2
do
    echo "Streaming stock prices crashed with exit code $?. Respawning..." >&2
    sleep 1
done
