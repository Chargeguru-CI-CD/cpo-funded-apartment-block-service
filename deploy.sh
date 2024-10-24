aws ecr get-login-password --region eu-west-3 | docker login --username AWS --password-stdin 336272500367.dkr.ecr.eu-west-3.amazonaws.com
docker build -t cpo-funded-apartment-block-service .
docker tag cpo-funded-apartment-block-service:latest 336272500367.dkr.ecr.eu-west-3.amazonaws.com/cpo-funded-apartment-block-service:latest
docker push 336272500367.dkr.ecr.eu-west-3.amazonaws.com/cpo-funded-apartment-block-service:latest