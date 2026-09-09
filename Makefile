.PHONY: up down logs test migrate lint typecheck

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f app worker

test:
	docker compose run --rm app pytest

migrate:
	docker compose run --rm migrate

lint:
	docker compose run --rm app ruff check .

typecheck:
	docker compose run --rm app mypy app workers
