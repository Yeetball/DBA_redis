FROM python:3

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev && \
    python3 -m pip install requests && \
    pip install --upgrade pip && \
    pip install pandas && \
    apt-get install -y apt-utils && \
    pip install pymongo && \
    pip install redis && \
    pip install bs4

COPY redisscraper.py /

CMD ["python3", "redisscraper.py"]