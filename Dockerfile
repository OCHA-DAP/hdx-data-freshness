FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN curl -so /root/runfreshness.sh \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/runfreshness.sh && \
    apk add --no-cache --update postgresql-dev && \
    pip --no-cache-dir install hdx-data-freshness && \
    apk del build-base postgresql-dev && \
    apk add --no-cache libpq && \
    rm -r /root/.cache && \
    rm -rf /var/lib/apk/*

CMD ["python3", "-m", "freshness"]
