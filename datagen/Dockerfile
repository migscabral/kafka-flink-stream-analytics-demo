FROM python:3.10-slim

WORKDIR /app

COPY clickstream_generator.py .

RUN pip install kafka-python

ENV KAFKA_BROKER=broker:9092 \
    KAFKA_TOPIC=clickstream \
    CLIENT_ID=clickstream-gen-1

CMD ["python", "-u", "clickstream_generator.py"]
