FROM public.ecr.aws/unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

RUN apk add --no-cache --upgrade --virtual .build-deps \
        build-base \
        libffi-dev \
        python3-dev \
        postgresql-dev && \
    pip --no-cache-dir install hdx-data-freshness && \
    apk del .build-deps && \
    apk add --no-cache libpq && \
    rm -rf /var/lib/apk/*

CMD ["python3", "-m", "hdx.freshness.app"]
