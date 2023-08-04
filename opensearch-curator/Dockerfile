FROM python:3-slim-buster

RUN pip install curator-opensearch==0.0.10

WORKDIR /opt/bitnami/python/bin/
RUN cp /usr/local/bin/curator $(pwd)

#RUN mkdir /dee 
ENTRYPOINT [ "/opt/bitnami/python/bin/curator" ]
