## Запуск приложения netflow_producer

docker build -t netflow_producer -f Dockerfile.netflow-producer .
docker run --rm -v ./logs:/netflow_producer/logs -v ./data:/data -it netflow_producer
