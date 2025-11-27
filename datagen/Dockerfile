FROM python:3.10-slim

WORKDIR /app

COPY clickstream_generator.py .

RUN pip install kafka-python

CMD ["python", "-u", "clickstream_generator.py"]
