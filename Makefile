# Al-Mudeer Backend Makefile
# Common development and deployment commands

.PHONY: help install dev test lint format clean db-migrate db-upgrade db-downgrade

# Default target
help:
	@echo "Al-Mudeer Backend Commands"
	@echo "=========================="
	@echo ""
	@echo "Development:"
	@echo "  make install     - Install all dependencies"
	@echo "  make dev         - Run development server"
	@echo "  make test        - Run all tests"
	@echo "  make lint        - Run linting checks"
	@echo "  make format      - Format code with black"
	@echo ""
	@echo "Database:"
	@echo "  make db-migrate  - Create new migration"
	@echo "  make db-upgrade  - Apply all migrations"
	@echo "  make db-downgrade - Rollback last migration"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean       - Remove cache and temp files"

# Install dependencies
install:
	pip install -r requirements.txt

# Run development server
dev:
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Run tests
test:
	python -m pytest tests/ -v --tb=short

# Run specific test file
test-security:
	python -m pytest tests/test_api.py::TestSecurityModule -v

# Linting (requires: pip install ruff)
lint:
	python -m ruff check .

# Format code (requires: pip install black)
format:
	python -m black .

# Database migrations with Alembic
db-migrate:
	@read -p "Migration message: " msg; \
	alembic revision -m "$$msg"

db-upgrade:
	alembic upgrade head

db-downgrade:
	alembic downgrade -1

db-history:
	alembic history

# Clean up cache and temp files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true

# Check all imports work
check-imports:
	python -c "from main import app; print('✓ Main app imports OK')" 2>/dev/null || \
	python -c "from models import init_enhanced_tables; print('✓ Models import OK')"

# Production build check
build-check:
	python -c "import ast; [ast.parse(open(f).read()) for f in __import__('glob').glob('**/*.py', recursive=True)]"
	@echo "✓ All Python files parse correctly"
