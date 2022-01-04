until python3 src/stream_comments.py --subreddit Bitcoin --project reddit-historical-336415 --dataset reddit_historical --table 01streaming_comments_2 &
do
    echo "Streaming comments crashed with exit code $?. Respawning..." >&2
    sleep 1
done
