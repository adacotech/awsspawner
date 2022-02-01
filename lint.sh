#!/bin/sh

poetry run mypy .
poetry run flake8 --show-source .
poetry run isort awsspawner/awsspawner.py
poetry run black awsspawner/awsspawner.py
