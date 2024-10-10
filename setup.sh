#!/bin/sh
set -e
uv sync
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'
echo "${YELLOW}Please activate the virtual environment now and build sattel:
${GREEN}source ${CYAN}.venv/bin/activate
${GREEN}./build.sh${RESET}"
