#!/usr/bin/env bash

set -e
set -x

poetry run pytest --cov=mongo_odm --cov=tests --cov-report=xml ${@}
bash ./scripts/lint.sh
