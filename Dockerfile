FROM ubuntu:latest
MAINTAINER Andrius Bacianskas "a.bacianskas@gmail.com"
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD ./requirements-lock.txt /usr/src/app/requirements.txt

RUN pip install -r requirements.txt

ADD . /usr/src/app

CMD make start-uwsgi & make start-flask
