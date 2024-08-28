#!/bin/bash

# JSON payload
json_payload='{
  "time": "2022-03-22T12:15:00.000Z",
  "cookie": "5",
  "country": "US",
  "device": "PC",
  "action": "VIEW",
  "origin": "example_origin",
  "product_info": {
    "product_id": "123",
    "brand_id": "brand_456",
    "category_id": "category_789",
    "price": 100
  }
}'

# Loop to send 500 POST requests
for i in {1..1000}
do
   curl -X POST -H "Content-Type: application/json" -d "$json_payload" http://localhost:8080/user_tags
   echo "Request $i sent"
done
