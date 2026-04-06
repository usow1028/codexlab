#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="${CODEXLAB_INSTALL_DIR:-$HOME/bin}"
SHELL_NAME="${CODEXLAB_SHELL_NAME:-$(basename "${SHELL:-bash}")}"
FISH_CONF_DIR="${CODEXLAB_FISH_CONF_DIR:-$HOME/.config/fish/conf.d}"
SKIP_PATH=0

usage() {
  cat <<'EOF'
Usage: bash scripts/install-codexlab.sh [--install-dir DIR] [--shell-rc FILE] [--skip-path]

Installs a stable codexlab wrapper into a user bin directory.
By default it creates:
  ~/bin/codexlab -> <repo>/bin/codexlab

Options:
  --install-dir DIR  Override the install directory. Default: ~/bin
  --shell-rc FILE    Override the shell rc file for bash/zsh installs
  --skip-path        Do not modify the shell rc file even if ~/bin is not on PATH
  --help             Show this help
EOF
}

default_shell_rc() {
  case "$SHELL_NAME" in
    zsh)
      printf '%s\n' "$HOME/.zshrc"
      ;;
    *)
      printf '%s\n' "$HOME/.bashrc"
      ;;
  esac
}

SHELL_RC="${CODEXLAB_SHELL_RC:-$(default_shell_rc)}"

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

UPDATED_PATH=0
PATH_TARGET=""
SOURCE_HINT=""
if [[ "$SKIP_PATH" -eq 0 && "$INSTALL_DIR" == "$HOME/bin" ]]; then
  case "$SHELL_NAME" in
    fish)
      PATH_TARGET="$FISH_CONF_DIR/codexlab_path.fish"
      SOURCE_HINT="source \"$PATH_TARGET\""
      mkdir -p "$FISH_CONF_DIR"
      FISH_PATH_SNIPPET=$'if not contains -- "$HOME/bin" $PATH\n    set -gx PATH "$HOME/bin" $PATH\nend\n'
      if [[ ! -f "$PATH_TARGET" ]] || [[ "$(cat "$PATH_TARGET")"$'\n' != "$FISH_PATH_SNIPPET" ]]; then
        printf '%s' "$FISH_PATH_SNIPPET" > "$PATH_TARGET"
        UPDATED_PATH=1
      fi
      ;;
    *)
      PATH_TARGET="$SHELL_RC"
      SOURCE_HINT="source \"$SHELL_RC\""
      PATH_LINE='export PATH="$HOME/bin:$PATH"'
      touch "$SHELL_RC"
      if ! grep -Fqx "$PATH_LINE" "$SHELL_RC"; then
        printf '\n%s\n' "$PATH_LINE" >> "$SHELL_RC"
        UPDATED_PATH=1
      fi
      ;;
  esac
fi

echo "Installed: $INSTALL_DIR/codexlab -> $REPO_ROOT/bin/codexlab"
if [[ "$UPDATED_PATH" -eq 1 ]]; then
  echo "Updated PATH in: $PATH_TARGET"
  echo "Run: $SOURCE_HINT"
elif [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
  echo "Your current PATH does not include: $INSTALL_DIR"
  if [[ "$INSTALL_DIR" == "$HOME/bin" ]]; then
    if [[ -n "$SOURCE_HINT" ]]; then
      echo "Run: $SOURCE_HINT"
    else
      echo "Open a new shell after adding $INSTALL_DIR to PATH."
    fi
  else
    echo "Add it manually before using codexlab."
  fi
fi

echo "Smoke test:"
echo "  codexlab doctor"
