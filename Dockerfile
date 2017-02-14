FROM unocha/alpine-base-python3

MAINTAINER Michael Rans <rans@email.com>

RUN apk update && \
    apk upgrade python3 && \
    apk add build-base musl-dev python3-dev libffi-dev postgresql-dev && \
    mkdir /hdx-data-freshness && \
    cd /hdx-data-freshness && \
    curl -so /hdx-data-freshness/requirements.txt \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/requirements.txt && \
    pip install -r requirements.txt && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

WORKDIR "/hdx-data-freshness"

CMD [ "python3", "/hdx-data-freshness/run.py", "postgresql://$DB_USER:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME" ]
