#!/bin/sh
grep -a 'published msgs' chana.log | awk '{ sum += $6; } END { print sum; }'
grep -a 'delivered msgs' chana.log | awk '{ sum += $6; } END { print sum; }'
