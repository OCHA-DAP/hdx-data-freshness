FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade --virtual .build-deps \
        build-base \
        python3-dev \
        postgresql-dev \
        py3-wheel && \
    pip --no-cache-dir install hdx-data-freshness && \
    apk del .build-deps && \
    apk add --no-cache libpq && \
    rm -rf /var/lib/apk/*

CMD ["python3", "-m", "hdx.freshness"]
