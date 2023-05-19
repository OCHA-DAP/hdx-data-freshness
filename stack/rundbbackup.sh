#!/bin/sh

today=$(date +%Y%m%d-%H%M%S)
daystokeep=7
cname=db
projdir=/data/freshness/stack
logdir=/data/freshness/log
logbasefname=crondbbackup
basedir=/backup
docoex="/usr/local/bin/docker-compose exec -T"

# backup
cd $projdir
$docoex $cname sh -c "cd $basedir; pg_dump -vFc -U freshness -f $today.pgsql freshness"

if [ "$?" -ne "0" ]; then
    echo "Database dump errored out. Removing the dump and exiting early."
    $docoex $cname sh -c "cd $basedir; rm -f $today.pgsql"
    exit 1
fi

# disabled by Serban on 2020.07.23. we are using custom dump format which is compressed by default.
#echo "Compressing the database dump..."
#$docoex $cname sh -c "cd $basedir; gzip $today.pgsql"

# we rely on logrotate from now on.
#echo "Copying this log file to a timestamped logfile..."
#cp -a $logdir/$logbasefname.log $logdir/$logbasefname.$today.log

# sync
#rsync -av /data/homes/freshness/freshness-db/*.psql.gz DSTIP:/DST/FOLDER/

# added by Serban on 2020.07.23
# keep only the last $daystokeep backups
$docoex $cname sh -c "cd $basedir; ls -1tr *pgsql | head -n -$daystokeep | xargs --no-run-if-empty rm -v"

# end
end=$(date +%Y%m%d-%H%M%S)

echo "Started at $today, ended at $end."
