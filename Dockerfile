FROM python:3.12-slim

WORKDIR /opt

COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

COPY ./webapp /opt/webapp