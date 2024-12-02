#!/bin/bash
# Call the original entrypoint script with its arguments
exec /entrypoint.sh "$@"

# Process initialization scripts in /docker-entrypoint-initdb.d
if [ -d "/docker-entrypoint-initdb.d" ]; then
  echo "Executing scripts in /docker-entrypoint-initdb.d"
  
else
  echo "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
fi

