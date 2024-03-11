ARG PG_MAJOR=16
FROM docker.io/postgres:$PG_MAJOR-bookworm

LABEL name="PostgreSQL Container Images" \
      vendor="The CloudNativePG Contributors and Tobias Florek" \
      version="${PG_MAJOR}" \
      summary="PostgreSQL Container images." \
      description="This Docker image contains PostgreSQL and Barman Cloud based on Postgres ${PG_MAJOR}-bookworm."

RUN apt-get update \
 && apt-get install -y curl \
 && curl -sSLo /etc/apt/trusted.gpg.d/ibotty-postgresql-public.asc https://ibotty.github.io/postgresql-related-packages/ibotty-postgresql-public.asc \
 && curl -sSLo /etc/apt/sources.list.d/ibotty-postgresql-public.list https://ibotty.github.io/postgresql-related-packages/ibotty-postgresql-public.list \
 && curl -sSL https://install.citusdata.com/community/deb.sh | bash \
 && apt-get update \
 && apt-get dist-upgrade \
 && apt-get install -y \
                       runit awscli zstd jq \
                       "postgresql-${PG_MAJOR}-pgaudit" \
                       "postgresql-${PG_MAJOR}-pgvector" \
                       "postgresql-${PG_MAJOR}-pg-failover-slots" \
                       barman-cli barman-cli-cloud \
                       "pg-ivm-pg${PG_MAJOR}" \
                       "pg-financial-pg${PG_MAJOR}" \
                       "pg-uuidv7-pg${PG_MAJOR}" \
                       "postgresql-plpython3-${PG_MAJOR}" \
                       "postgresql-${PG_MAJOR}-citus-12.1" \
                       "postgresql-${PG_MAJOR}-rum" \
                       "postgresql-${PG_MAJOR}-first-last-agg" \
                       "supabase-wrappers-pg${PG_MAJOR}" \
                       "pg-row-hashes-pg${PG_MAJOR}" \
                       "postgresql-${PG_MAJOR}-tds-fdw" \
                       "duckdb-fdw-pg${PG_MAJOR}" \
 && apt-get install -y python3-pip \
 && pip install --break-system-packages python-snappy \
 && apt-get remove -y --autoremove python3-pip \
 && (cd /usr/bin && ln -s chpst /usr/bin/envdir) \
 && apt-get clean \
 && rm -rf /tmp/* /var/lib/apt/lists/*

# Change the uid of postgres to 26
RUN usermod -u 26 postgres
USER 26
