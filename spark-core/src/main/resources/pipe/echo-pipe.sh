#!/usr/bin/env bash

echo 'start'

while read host; do
    echo `ping -c 1 $host`
done

exit 0
