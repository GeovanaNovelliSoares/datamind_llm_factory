.PHONY: help install up down dev ui test lint seed

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:  ## Install all dependencies
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install streamlit plotly pytest pytest-asyncio pytest-cov httpx ruff

up:  ## Start full stack with Docker Compose
	docker compose up --build -d
	@echo "✓ API:      http://localhost:8000/docs"
	@echo "✓ UI:       http://localhost:8501"
	@echo "✓ Grafana:  http://localhost:3000"
	@echo "✓ Flower:   http://localhost:5555"
	@echo "✓ MLflow:   http://localhost:5000"

down:  ## Stop Docker Compose
	docker compose down

dev:  ## Start API locally (needs Redis + Postgres running)
	uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

worker:  ## Start Celery worker locally
	celery -A src.workers.celery_app.celery_app worker --loglevel=info --queues=etl --pool=solo

ui:  ## Start Streamlit UI
	streamlit run app/streamlit_app.py --server.port 8501

seed:  ## Generate sample SaaS data CSV
	python scripts/generate_sample_data.py --rows 500 --output data/saas_sample.csv
	@echo "✓ Sample data: data/saas_sample.csv"

test:  ## Run all tests
	pytest tests/unit/ tests/integration/ -v --cov=src --cov-report=term-missing

test-unit:  ## Unit tests only
	pytest tests/unit/ -v

lint:  ## Lint and format
	ruff check src/ tests/ --fix && ruff format src/ tests/

clean:  ## Remove generated data
	rm -rf data/ mlflow_tracking/ .pytest_cache/ htmlcov/ coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
