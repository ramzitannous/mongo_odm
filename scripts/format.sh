#!/bin/sh -e
set -x

poetry run autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place mongo_odm tests --exclude=__init__.py
poetry run black mongo_odm tests
poetry run isort --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 92 --known-local-folder mongo_odm tests
