#!/bin/sh
cp /dev/null $(docker inspect -f '{{.LogPath}}' hdxdatafreshnessdocker_app_1)
python3 -m freshness