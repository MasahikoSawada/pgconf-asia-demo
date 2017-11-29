VERSION=${demo} # "demo" or "10"
ACTION=""

# Parse options
while getopts v:a: OPT
do
    case $OPT in
	v)
	    VERSION=$OPTARG
	    ;;
	a)
	    ACTION=$OPTARG
	    ;;
    esac
done

if [ "$VERSION" == "demo" ];then
    PGBIN=/home/masahiko/pgsql/demo/bin
    PGHOME=/home/masahiko/pgsql/demo
elif [ "$VERSION" == "10" ];then
    PGBIN=/home/masahiko/pgsql/10.1/bin
    PGHOME=/home/masahiko/pgsql/10.1
else
    echo "invalid version: \"${VERSION}\". Must be \"demo\" or \"10\""
    exit 0
fi

if [ "$ACTION" == "" ];then
    echo "invalid action: \"${ACTION}\""
    exit 0
fi

# Setting environment variables
DBNAME=postgres
PSQL="$PGBIN/psql -d postgres"

PGDATAS="coord shd1 shd2 shd3 shd4"
port=4440
for data in ${PGDATAS}
do
    pgdata=${PGHOME}/${data}
    ${PGBIN}/pg_ctl ${ACTION} -D ${pgdata}
done
