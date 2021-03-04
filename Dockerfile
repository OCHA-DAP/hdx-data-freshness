FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade --virtual .build-deps1 \
        build-base \
        python3-dev \
        postgresql-dev && \
    apk add --no-cache --upgrade -X http://dl-cdn.alpinelinux.org/alpine/edge/community --virtual .build-deps2 \
        py3-wheel && \
    pip --no-cache-dir install hdx-data-freshness && \
    apk del .build-deps1 && \
    apk del .build-deps2 && \
    apk add --no-cache libpq && \
    rm -rf /var/lib/apk/*

CMD ["python3", "-m", "hdx.freshness"]
