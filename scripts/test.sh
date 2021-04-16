#!/usr/bin/env bash

set -e
set -x

poetry run pytest --cov=motorodm --cov=tests --cov-report=xml ${@}
bash ./scripts/lint.sh
