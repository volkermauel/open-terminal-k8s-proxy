.PHONY: help install test lint format clean docker-build docker-push

help:
	@echo "Available commands:"
	@echo "  install        Install dependencies"
	@echo "  test           Run tests"
	@echo "  lint           Run linting (ruff + mypy)"
	@echo "  format         Format code with ruff"
	@echo "  clean          Clean build artifacts"
	@echo "  docker-build   Build Docker image"
	@echo "  docker-push    Push Docker image"

install:
	pip install -e ".[dev]"

test:
	pytest tests -v --tb=short

lint:
	ruff check terminal_proxy tests
	mypy terminal_proxy

format:
	ruff format terminal_proxy tests
	ruff check --fix terminal_proxy tests

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-build:
	docker build -t open-terminal-k8s-proxy:latest .

docker-push:
	docker push open-terminal-k8s-proxy:latest
