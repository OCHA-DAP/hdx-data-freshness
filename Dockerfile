FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN mkdir /hdx-data-freshness && \
    cd /hdx-data-freshness && \
    curl -so /hdx-data-freshness/requirements.txt \
        https://raw.githubusercontent.com/OCHA-DAP/hdx-data-freshness/master/requirements.txt && \
    pip install -r requirements.txt

ADD run.py /srv/

WORKDIR "/srv"

RUN chmod u+x run.py

CMD [ "run.py" ]
