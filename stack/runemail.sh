#!/bin/sh

basedir=/data/freshness
projdir=$basedir/stack
doco=/usr/local/bin/docker-compose
cname=freshness_email_1

cd $projdir
$doco run --rm email
