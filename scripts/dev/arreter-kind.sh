#!/usr/bin/env bash
# Supprime le cluster kind local (et toutes ses données).
set -euo pipefail
kind delete cluster --name "${CLUSTER:-lakehouse}"
