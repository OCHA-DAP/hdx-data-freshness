FROM public.ecr.aws/unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

RUN apk add --no-cache --upgrade --virtual .build-deps \
        build-base \
        libffi-dev \
        postgresql-dev \
        python3-dev \
        py3-wheel && \
    apk del .build-deps && \
    apk add --no-cache libpq && \
    rm -rf /var/lib/apk/*

CMD ["PYTHONPATH=src:\$PYTHONPATH", "python3", "-m", "freshness.app"]
