# Defaults
VERSION=demo # "demo" or "10"
TYPE=list

function log()
{
    echo -e "\033[0;36m${@}\033[0;39m"
}

function usage()
{
    echo "Usage:"
    echo "$ ./setup.sh -v <version> -t <type>"
    echo "    -v VERSION : \"10\" or \"demo\""
    echo "    -t TYPE : \"list\" or \"hash\""
}

# Parse options
while getopts v:t: OPT
do
    case $OPT in
	v)
	    VERSION=$OPTARG
	    ;;
	t)
	    TYPE=$OPTARG
	    ;;
    esac
done

# Setting enviorment variables
if [ "$VERSION" == "demo" ];then
    PGBIN=/home/masahiko/pgsql/demo/bin
    PGHOME=/home/masahiko/pgsql/demo
    pri_port=4440
    port=4440
elif [ "$VERSION" == "10" ];then
    PGBIN=/home/masahiko/pgsql/10.1/bin
    PGHOME=/home/masahiko/pgsql/10.1
    pri_port=3330
    port=3330
else
    echo "invalid version: \"${VERSION}\". Must be \"demo\" or \"10\""
    usage
    exit 0
fi

# Check type: list or hash
if [ "$TYPE" != "hash" -a "$TYPE" != "list" ];then
    echo "invalid type: \"${TYPE}\". Must be \"hash\" or \"list\""
    usage
    exit 0
elif [ "$TYPE" == "hash" -a "$VERSION" == "10" ];then
    echo "hash parition doesn't support in PostgreSQL 10"
    usage
    exit 0
fi

# Setting environment variables
DBNAME=postgres
PSQL="$PGBIN/psql -d postgres"

# Show current setting
log "[ Version = \"${VERSION}\", Type = \"${TYPE}\" ]"

PGDATAS="coord shd1 shd2 shd3 shd4"
for data in ${PGDATAS}
do
    pgdata=${PGHOME}/${data}
    log "cleaning up ${data} ..."

    ${PGBIN}/pg_ctl stop -D ${pgdata} > /dev/null 2>&1
    rm -r ${pgdata}
    ${PGBIN}/initdb -D ${pgdata} -E UTF8 --no-locale > /dev/null 2>&1

    cat <<EOF >> ${pgdata}/postgresql.conf
log_line_prefix = '<${data}> [%p] '
max_prepared_transactions = 10
shared_preload_libraries = 'pg_simula'
#log_statement = 'all'
EOF

    # Setting only on "demo"
    if [ "$VERSION" == "demo" ];then
	cat <<EOF >> ${pgdata}/postgresql.conf
max_prepared_transactions = 10
max_foreign_transaction_resolvers = 10
max_prepared_foreign_transactions = 10
EOF
    fi

    ${PGBIN}/pg_ctl start -D ${pgdata} -o "-p ${port}" -c

    # Set up pg_simula on each servers
    ${PSQL} -p ${port} -c "create extension pg_simula" > /dev/null 2>&1
    cat <<EOF | ${PSQL} -p ${port} > /dev/null 2>&1
select add_simula_event('COMMIT', 'error', 0);
select add_simula_event('PREPARE TRANSACTION', 'error', 0);
EOF

    log "${data} created"

    # next port number
    port=$(($port + 1))
done

# Set up server, schemas and data
if [ "$VERSION" == "demo" ];then
    log "Setting up foreign servers.."
    ${PSQL} -p ${pri_port} -f ./setup-scripts/00_shard-server-setup_pg11.sql > /dev/null 2>&1
    log "Creating schemas.."
    ${PSQL} -p ${pri_port} -f ./setup-scripts/01-shard-tables-list_pg11.sql > /dev/null 2>&1
    log "Loading data.."
else
    log "Setting up foreign servers.."
    ${PSQL} -p ${pri_port} -f ./setup-scripts/00_shard-server-setup_pg10.sql > /dev/null 2>&1
    log "Creating schemas.."
    ${PSQL} -p ${pri_port} -f ./setup-scripts/01-shard-tables-list_pg10.sql > /dev/null 2>&1
    log "Loading data.."
fi

${PSQL} -p ${pri_port} -f ./setup-scripts/02_shard-data-in.sql > /dev/null 2>&1
log "Enable atomic commit.."
${PSQL} -p ${pri_port} -f ./setup-scripts/03_enable-twophase-commit.sql > /dev/null 2>&1
log "Done, checking.."
${PSQL} -p ${pri_port} -f ./setup-scripts/04_check.sql
