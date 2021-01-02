#!/usr/bin/env bash

set -e
set -x

poetry run mypy motor_odm --disallow-untyped-defs
poetry run black motor_odm tests --check
poetry run isort . --multi-line=3 --trailing-comma --force-grid-wrap=0 --combine-as --line-width 92 --check-only --known-local-folder motor_odm tests
