FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN apk update && \
    apk add build-base && \
    curl -so /srv/requirements.txt \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/requirements.txt && \
    pip install -r /srv/requirements.txt && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

ADD run.py /srv/

WORKDIR "/srv"

RUN chmod u+x run.py

CMD [ "run.py" ]
