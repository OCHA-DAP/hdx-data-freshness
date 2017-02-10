FROM unocha/alpine-base-python3

MAINTAINER Michael Rans <rans@email.com>

RUN apk update && \
    apk upgrade python3 && \
    apk add postgresql-dev libffi-dev python3-dev musl-dev build-base && \
    mkdir /hdx-data-freshness && \
    cd /hdx-data-freshness && \
    curl -so /hdx-data-freshness/requirements.txt \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/requirements.txt && \
    pip install -r requirements.txt && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

#ADD . /hdx-data-freshness

WORKDIR "/hdx-data-freshness"

CMD [ "python3", "/hdx-data-freshness/run.py" ]
