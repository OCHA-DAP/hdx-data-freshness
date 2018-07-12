FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --update build-base python3-dev postgresql-dev && \
    pip --no-cache-dir install hdx-data-freshness && \
    apk del build-base python3-dev postgresql-dev && \
    apk add --no-cache libpq && \
    rm -r /root/.cache && \
    rm -rf /var/lib/apk/*

CMD ["python3", "-m", "hdx.freshness"]
