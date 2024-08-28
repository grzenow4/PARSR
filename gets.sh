#!/bin/bash

# Endpoint parameters
cookie="example_cookie"
time_range="2022-03-22T12:00:00.000_2022-03-22T12:30:00.000"
limit=200

# JSON payload (optional, but since it's required=false, you can omit or provide it)
json_payload='{
  "cookie": "example_cookie",
  "views": [],
  "buys": []
}'

# Make the POST request
curl -X POST "http://localhost:8080/user_profiles/${cookie}?time_range=${time_range}&limit=${limit}" \
     -H "Content-Type: application/json" \
     -d "$json_payload"

echo "User profile request sent for cookie: $cookie"
