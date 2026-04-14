.PHONY: help build up down logs clean

help:
	@echo "Available commands:"
	@echo "  make build    - Build Docker images"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - View logs"
	@echo "  make crawl    - Run crawler once"
	@echo "  make clean    - Remove containers and volumes"

build:
	docker-compose build

up:
	docker-compose up -d postgres api
	@echo "Services started:"
	@echo "  API: http://localhost:8000"
	@echo "  API Docs: http://localhost:8000/docs"
	@echo "  PostgreSQL: localhost:5433"

down:
	docker-compose down

logs:
	docker-compose logs -f

crawl:
	docker-compose --profile crawler run --rm crawler

shell:
	docker-compose exec api bash

psql:
	docker-compose exec postgres psql -U crawler_user -d tri_layer_crawler

clean:
	docker-compose down -v
	rm -rf data/raw/*.csv data/raw/*.json