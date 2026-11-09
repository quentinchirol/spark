#!/usr/bin/env bash
# Exécuté une fois à la création du Codespace / dev container.
set -euo pipefail
cd "$(dirname "$0")/.."

python -m pip install --upgrade pip
pip install -r python/requirements.txt ruff

cat <<'EOF'

Environnement prêt.
  - Tests unitaires        : cd python && pytest tests/unitaires
  - Stack complète (kind)  : scripts/dev/demarrer-kind.sh
  - Arrêter la stack       : scripts/dev/arreter-kind.sh
EOF
