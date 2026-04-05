#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="${CODEXLAB_INSTALL_DIR:-$HOME/bin}"
SHELL_RC="${CODEXLAB_SHELL_RC:-$HOME/.bashrc}"
SKIP_PATH=0

usage() {
  cat <<'EOF'
Usage: bash scripts/install-codexlab.sh [--install-dir DIR] [--shell-rc FILE] [--skip-path]

Installs a stable codexlab wrapper into a user bin directory.
By default it creates:
  ~/bin/codexlab -> <repo>/bin/codexlab

Options:
  --install-dir DIR  Override the install directory. Default: ~/bin
  --shell-rc FILE    Shell rc file to update when using ~/bin. Default: ~/.bashrc
  --skip-path        Do not modify the shell rc file even if ~/bin is not on PATH
  --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    --shell-rc)
      SHELL_RC="$2"
      shift 2
      ;;
    --skip-path)
      SKIP_PATH=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$INSTALL_DIR"
ln -sfn "$REPO_ROOT/bin/codexlab" "$INSTALL_DIR/codexlab"

PATH_LINE='export PATH="$HOME/bin:$PATH"'
UPDATED_PATH=0
if [[ "$SKIP_PATH" -eq 0 && "$INSTALL_DIR" == "$HOME/bin" ]]; then
  touch "$SHELL_RC"
  if ! grep -Fqx "$PATH_LINE" "$SHELL_RC"; then
    printf '\n%s\n' "$PATH_LINE" >> "$SHELL_RC"
    UPDATED_PATH=1
  fi
fi

echo "Installed: $INSTALL_DIR/codexlab -> $REPO_ROOT/bin/codexlab"
if [[ "$UPDATED_PATH" -eq 1 ]]; then
  echo "Updated PATH in: $SHELL_RC"
  echo "Run: source \"$SHELL_RC\""
elif [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
  echo "Your current PATH does not include: $INSTALL_DIR"
  if [[ "$INSTALL_DIR" == "$HOME/bin" ]]; then
    echo "Run: source \"$SHELL_RC\""
  else
    echo "Add it manually before using codexlab."
  fi
fi

echo "Smoke test:"
echo "  codexlab doctor"
