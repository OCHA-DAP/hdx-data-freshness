FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN echo "@testing http://dl-3.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk update && \
    apk add postgresql-dev dockerize@testing && \
    pip install https://github.com/ocha-dap/hdx-data-freshness/zipball/master#egg=hdx-data-freshness && \
    curl -so /etc/cron.d/freshness-cron \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/crontab && \
    chmod 0644 /etc/cron.d/freshness-cron && \
    touch /var/log/cron.log && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

CMD ["cron", "&&", "tail", "-f", "/var/log/cron.log"]
