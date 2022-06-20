#!/bin/bash 
docker stop ds
docker rm ds
docker run -d --name ds -p 80:80 nonameleft/fastapi_data
