FROM unocha/alpine-base-python3

MAINTAINER Michael Rans <rans@email.com>

RUN apk update

RUN apk upgrade python3

RUN apk add postgresql-dev libffi-dev python3-dev musl-dev build-base

ADD . /hdx-data-freshness

WORKDIR "/hdx-data-freshness"

RUN pip install -r requirements.txt

RUN apk del build-base

RUN rm -rf /var/cache/apk/*

CMD [ "python3", "/hdx-data-freshness/run.py" ]
