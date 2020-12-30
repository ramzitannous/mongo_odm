#!/usr/bin/env bash

poetry run mkdocs build

cp ./docs/index.md ./README.md
