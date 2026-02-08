FROM python:3.9-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget && \
    apt-get clean

RUN pip install pyspark

RUN mkdir -p /dependencies
RUN wget -O /dependencies/postgresql-42.7.2.jar https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

WORKDIR /app
COPY . /app

CMD ["python", "main.py"]