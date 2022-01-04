until python3 src/stream_submissions.py --subreddit Bitcoin --project reddit-historical-336415 --dataset reddit_historical --table 01streaming_submissions_2 &
do
    echo "Streaming submissions crashed with exit code $?. Respawning..." >&2
    sleep 1
done
