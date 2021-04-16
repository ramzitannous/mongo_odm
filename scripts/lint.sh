#!/usr/bin/env bash

set -e
set -x

poetry run mypy motorodm --disallow-untyped-defs
poetry run black motorodm tests --check
poetry run isort --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 92 --known-local-folder motorodm tests
