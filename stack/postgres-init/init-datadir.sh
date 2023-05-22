#!/bin/bash
set -e

BASEDIR=/var/lib/pgsql

mkdir -p $BASEDIR      && chown 70:70 $BASEDIR
mkdir -p $BASEDIR/data && chown 70:70 $BASEDIR/data
