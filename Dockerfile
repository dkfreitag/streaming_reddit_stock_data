FROM python:3.10.1

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/bash", "./sh/stream_all.sh"]