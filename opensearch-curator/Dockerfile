FROM python:3-slim-buster
RUN pip install curator-opensearch==0.0.7

RUN useradd --create-home curator
WORKDIR /home/curator
USER curator

RUN /usr/local/bin/curator --version
RUN /usr/local/bin/curator --help

ENTRYPOINT [ "/usr/local/bin/curator" ]
