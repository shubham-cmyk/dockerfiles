#! /usr/bin/env bash

set -x
IFS=$'\n\t'

## NOTE, these env needs to be passed in the cronjob
# PGHOST PGPASSWORD

ALL_DB_SIZE_QUERY="select sum(pg_database_size(datname)::numeric) from pg_database;"
PG_BIN=/usr/lib/postgresql/$PG_VERSION/bin
DUMP_SIZE_COEFF=5
ERRORCOUNT=0
POSTGRES_OPERATOR=spilo
LOGICAL_BACKUP_PROVIDER=${LOGICAL_BACKUP_PROVIDER:="s3"}
LOGICAL_BACKUP_S3_RETENTION_TIME=${LOGICAL_BACKUP_S3_RETENTION_TIME:=""}
LOGICAL_BACKUP_AZURE_RETENTION_TIME=${LOGICAL_BACKUP_AZURE_RETENTION_TIME:=""}

PG_DUMP_EXTRA_ARGUMENTS=${PG_DUMP_EXTRA_ARGUMENTS:=""}
PG_DUMPALL_EXTRA_ARGUMENTS=${PG_DUMPALL_EXTRA_ARGUMENTS:=""}
PG_DUMP_NJOBS=${PG_DUMP_NJOBS:-4}
PG_DUMP_COMPRESS=${PG_DUMP_COMPRESS:-6}
PGHOST=${PGHOST:-"keycloakx-pgsql"}

function estimate_size {
  "$PG_BIN"/psql -tqAc "${ALL_DB_SIZE_QUERY}"
}

function dump_db {
  echo "Taking dump of ${PGDATABASE} from ${PGHOST}"
  # settings are taken from the environment
  "$PG_BIN"/pg_dump
}

function dump_global {
  "$PG_BIN"/pg_dumpall --globals-only $PG_DUMPALL_EXTRA_ARGUMENTS
}

function list_databases {
    # SQL from dumpall
    psql -c "SELECT datname FROM pg_database d WHERE datallowconn AND datconnlimit != -2 ORDER BY (datname <> 'template1'), datname" --csv | tail -n +2
}

function compress {
  pigz
}

function az_upload {
  PATH_TO_BACKUP="${LOGICAL_BACKUP_AZURE_CONTAINER}/${POSTGRES_OPERATOR}/${LOGICAL_BACKUP_AZURE_SCOPE_SUFFIX}/logical_backups/$(date +%s).sql.gz"

  az storage blob upload --file "${1}" --account-name "${LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_NAME}" --account-key "${LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_KEY}" -c "${LOGICAL_BACKUP_AZURE_CONTAINER}" -n "${PATH_TO_BACKUP}"
}

function aws_delete_objects {
  args=(
    "--bucket=$LOGICAL_BACKUP_S3_BUCKET"
  )

  [[ ! -z "${LOGICAL_BACKUP_S3_ENDPOINT}" ]] && args+=("--endpoint-url=${LOGICAL_BACKUP_S3_ENDPOINT}")
  [[ ! -z "${LOGICAL_BACKUP_S3_REGION}" ]] && args+=("--region=${LOGICAL_BACKUP_S3_REGION}")

  aws s3api delete-objects "${args[@]}" --delete Objects=["$(printf {Key=%q}, "$@")"],Quiet=true
}
export -f aws_delete_objects

function aws_delete_outdated {
  if [[ -z "$LOGICAL_BACKUP_S3_RETENTION_TIME" ]] ; then
    echo "no retention time configured: skip cleanup of outdated backups"
    return 0
  fi

  # define cutoff date for outdated backups (day precision)
  cutoff_date=$(date -d "$LOGICAL_BACKUP_S3_RETENTION_TIME ago" +%F)

  # mimic bucket setup from Spilo
  prefix="${POSTGRES_OPERATOR}/"${LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX}"/logical_backups/"

  args=(
    "--no-paginate"
    "--output=text"
    "--prefix=$prefix"
    "--bucket=$LOGICAL_BACKUP_S3_BUCKET"
  )

  [[ ! -z "${LOGICAL_BACKUP_S3_ENDPOINT}" ]] && args+=("--endpoint-url=${LOGICAL_BACKUP_S3_ENDPOINT}")
  [[ ! -z "${LOGICAL_BACKUP_S3_REGION}" ]] && args+=("--region=${LOGICAL_BACKUP_S3_REGION}")

  # list objects older than the cutoff date
  aws s3api list-objects "${args[@]}" --query="Contents[?LastModified<='$cutoff_date'].[Key]" > /tmp/outdated-backups

  # spare the last backup
  sed -i '$d' /tmp/outdated-backups

  count=$(wc -l < /tmp/outdated-backups)
  if [[ $count == 0 ]] ; then
    echo "no outdated backups to delete"
    return 0
  fi
  echo "deleting $count outdated backups created before $cutoff_date"

  # deleted outdated files in batches with 100 at a time
  tr '\n' '\0'  < /tmp/outdated-backups | xargs -0 -P1 -n100 bash -c 'aws_delete_objects "$@"' _
}

function aws_upload {
  declare -r EXPECTED_SIZE="$1"

  # mimic bucket setup from Spilo
  # to keep logical backups at the same path as WAL
  # NB: $LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX already contains the leading "/" when set by the Postgres Operator
  PATH_TO_BACKUP="s3://${LOGICAL_BACKUP_S3_BUCKET}/${POSTGRES_OPERATOR}/${LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX}/logical_backups/$(date +%s).sql.gz"

  args=()

  [[ ! -z "${EXPECTED_SIZE}" ]] && args+=("--expected-size=${EXPECTED_SIZE}")
  [[ ! -z "${LOGICAL_BACKUP_S3_ENDPOINT}" ]] && args+=("--endpoint-url=${LOGICAL_BACKUP_S3_ENDPOINT}")
  [[ ! -z "${LOGICAL_BACKUP_S3_REGION}" ]] && args+=("--region=${LOGICAL_BACKUP_S3_REGION}")

  echo "Uploading dump to s3"
  aws s3 cp - "$PATH_TO_BACKUP" "${args[@]//\'/}"
}

function gcs_upload {
  PATH_TO_BACKUP=gs://${LOGICAL_BACKUP_S3_BUCKET}"/"${POSTGRES_OPERATOR}"/"${LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX}"/logical_backups/"$(date +%s).sql.gz

  gsutil -o Credentials:gs_service_key_file=${LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS} cp - "${PATH_TO_BACKUP}"
}

function upload {
  case $LOGICAL_BACKUP_PROVIDER in
    "gcs")
      gcs_upload
      ;;
    "s3")
      aws_upload $(($(estimate_size) / DUMP_SIZE_COEFF))
      aws_delete_outdated
      ;;
    "az")
      az_upload
      ;;
  esac
}

if [[ "$LOGICAL_BACKUP_PROVIDER" == "s3" && "$PGDATABASE" == "s3" ]]; then
    PATH_TO_BACKUP="s3://${LOGICAL_BACKUP_S3_BUCKET}/${POSTGRES_OPERATOR}/${LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX}/logical_backups/$(date +%Y-%m-%dT%H%M%S)"

    echo "Dumping and uploading global items..."
    dump_global | compress | aws_upload "$PATH_TO_BACKUP/global.sql.gz"

    [[ ${PIPESTATUS[0]} != 0 || ${PIPESTATUS[1]} != 0 || ${PIPESTATUS[2]} != 0 ]] && (( ERRORCOUNT += 1 ))
    set +x

    list_databases > /tmp/database-list
    while read dbname; do 
        echo "Dumping $dbname..."
        dump_db $dbname
        echo "Uploading directory /tmp/db-$dbname to $PATH_TO_BACKUP/$dbname..."
        aws_upload "/tmp/db-$dbname" "$PATH_TO_BACKUP/$dbname"
        echo "Cleaning up /tmp/db-$dbname..."
        rm -rf "/tmp/db-$dbname"
    done < /tmp/database-list

    rm /tmp/database-list

    exit $ERRORCOUNT
elif [[ "$LOGICAL_BACKUP_PROVIDER" == "az" && "$PGDATABASE" == "az" ]]; then
    PATH_TO_BACKUP="${POSTGRES_OPERATOR}/"$SCOPE$LOGICAL_BACKUP_AZURE_SCOPE_SUFFIX"/logical_backups/"$(date +%Y-%m-%dT%H%M%S)

    echo "Dumping and uploading global items..."
    dump_global | compress | az_upload "$PATH_TO_BACKUP/global.sql.gz"

    [[ ${PIPESTATUS[0]} != 0 || ${PIPESTATUS[1]} != 0 || ${PIPESTATUS[2]} != 0 ]] && (( ERRORCOUNT += 1 ))
    set +x

    list_databases > /tmp/database-list
    while read dbname; do 
        echo "Dumping $dbname..."
        dump_db $dbname
        echo "Uploading directory /tmp/db-$dbname to $PATH_TO_BACKUP/$dbname..."
        az_upload "/tmp/db-$dbname" "$PATH_TO_BACKUP/$dbname"
        echo "Cleaning up /tmp/db-$dbname..."
        rm -rf "/tmp/db-$dbname"
    done < /tmp/database-list

    rm /tmp/database-list

    exit $ERRORCOUNT
else
  dump_db | compress | upload
  [[ ${PIPESTATUS[0]} != 0 || ${PIPESTATUS[1]} != 0 || ${PIPESTATUS[2]} != 0 ]] && (( ERRORCOUNT += 1 ))
  set +x
  exit $ERRORCOUNT
fi
