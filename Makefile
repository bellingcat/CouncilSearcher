dev:
	docker compose -f docker-compose.base.yaml -f docker-compose.dev.yaml build
	docker compose -f docker-compose.base.yaml -f docker-compose.dev.yaml up

run: 
	docker compose -f docker-compose.base.yaml -f docker-compose.prod.yaml build
	docker compose -f docker-compose.base.yaml -f docker-compose.prod.yaml up -d --remove-orphans

stop:
	docker compose -f docker-compose.base.yaml -f docker-compose.prod.yaml down