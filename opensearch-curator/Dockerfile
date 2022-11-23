FROM python:3-slim-buster

RUN pip install curator-opensearch==0.0.7

WORKDIR /opt/bitnami/python/bin/
RUN cp /usr/local/bin/curator $(pwd)

ENTRYPOINT [ "/opt/bitnami/python/bin/curator" ]
