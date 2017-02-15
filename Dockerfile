FROM mcarans/hdx-python-api

MAINTAINER Michael Rans <rans@email.com>

RUN pip install https://github.com/ocha-dap/hdx-data-freshness/zipball/master#egg=hdx-data-freshness && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

CMD [ "python3", "-m", "freshness" ]
