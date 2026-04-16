.PHONY: help up down core-up clean

help:
	@echo "Lakehouse Setup Commands:"
	@echo "  make up          - Start all services (Warning: Heavy RAM usage)"
	@echo "  make core-up     - Start only core ingestion & storage (Kafka, MinIO, Spark)"
	@echo "  make down        - Stop all services"
	@echo "  make clean       - Stop services and remove attached volumes (wipes data)"

up:
	docker-compose --profile all up -d --build

core-up:
	docker-compose --profile core up -d

down:
	docker-compose down

clean:
	docker-compose down -v
