#!/usr/bin/env bash

set -e
set -x

poetry run mypy $1 --disallow-untyped-defs
poetry run autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place $1
poetry run black $1
poetry run isort --src=$1 --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 92 --known-local-folder mongo_odm tests
