#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ICON_PATH="$ROOT_DIR/resources/app_icon.icns"
SOURCE_ICON_PATH="$ROOT_DIR/icon.png"
SPEC_PATH="$ROOT_DIR/FXAutoTradeLab.spec"
DIST_APP_BUNDLE="$ROOT_DIR/dist/FXAutoTradeLab.app"
INSTALL_DIR="${FXAUTOTRADE_INSTALL_DIR:-$HOME/Applications}"
INSTALL_APP_BUNDLE="$INSTALL_DIR/FXAutoTradeLab.app"
STAGING_ROOT="${TMPDIR:-/tmp}/fxautotrade-app-staging"
STAGING_APP_BUNDLE="$STAGING_ROOT/FXAutoTradeLab.app"

ensure_macos_icon() {
  if [[ -f "$ICON_PATH" && ( ! -f "$SOURCE_ICON_PATH" || "$ICON_PATH" -nt "$SOURCE_ICON_PATH" ) ]]; then
    return
  fi
  if [[ ! -f "$SOURCE_ICON_PATH" ]]; then
    return
  fi
  if command -v sips >/dev/null 2>&1 && command -v iconutil >/dev/null 2>&1; then
    work_dir="$(mktemp -d)"
    iconset_dir="$work_dir/app_icon.iconset"
    mkdir -p "$iconset_dir"
    sips -z 16 16 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_16x16.png" >/dev/null
    sips -z 32 32 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_16x16@2x.png" >/dev/null
    sips -z 32 32 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_32x32.png" >/dev/null
    sips -z 64 64 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_32x32@2x.png" >/dev/null
    sips -z 128 128 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_128x128.png" >/dev/null
    sips -z 256 256 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_128x128@2x.png" >/dev/null
    sips -z 256 256 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_256x256.png" >/dev/null
    sips -z 512 512 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_256x256@2x.png" >/dev/null
    sips -z 512 512 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_512x512.png" >/dev/null
    sips -z 1024 1024 "$SOURCE_ICON_PATH" --out "$iconset_dir/icon_512x512@2x.png" >/dev/null
    if iconutil -c icns "$iconset_dir" -o "$ICON_PATH" >/dev/null 2>&1; then
      return
    fi
  fi
  for python_cmd in "$ROOT_DIR/.venv/bin/python" python3 "$ROOT_DIR/.venv_gui/bin/python"; do
    if [[ -x "$python_cmd" ]] || command -v "$python_cmd" >/dev/null 2>&1; then
      SOURCE_ICON_PATH="$SOURCE_ICON_PATH" ICON_PATH="$ICON_PATH" "$python_cmd" - <<'PY' && return
import os
from pathlib import Path
from PIL import Image

source = Path(os.environ["SOURCE_ICON_PATH"])
target = Path(os.environ["ICON_PATH"])
image = Image.open(source).convert("RGBA")
sizes = [(16, 16), (32, 32), (64, 64), (128, 128), (256, 256), (512, 512), (1024, 1024)]
image.save(target, format="ICNS", sizes=sizes)
PY
    fi
  done
  echo "warning: icon.png は見つかりましたが、app_icon.icns を更新できませんでした。" >&2
}

ensure_macos_icon

# Finder metadata on copied icon assets can make macOS bundle signing fail.
xattr -cr "$ICON_PATH" >/dev/null 2>&1 || true
if [[ -f "$SOURCE_ICON_PATH" ]]; then
  xattr -cr "$SOURCE_ICON_PATH" >/dev/null 2>&1 || true
fi

run_pyinstaller() {
  local pyinstaller_cmd="$1"
  local pyinstaller_config_dir="${TMPDIR:-/tmp}/fxautotrade-pyinstaller"
  mkdir -p "$pyinstaller_config_dir"
  PYINSTALLER_CONFIG_DIR="$pyinstaller_config_dir" \
    "$pyinstaller_cmd" \
      --noconfirm \
      "$SPEC_PATH"
}

resign_bundle() {
  local bundle_path="$1"
  if [[ -d "$bundle_path" ]] && command -v codesign >/dev/null 2>&1; then
    xattr -cr "$bundle_path" >/dev/null 2>&1 || true
    codesign --force --deep -s - "$bundle_path" >/dev/null 2>&1 || true
  fi
}

install_launchable_copy() {
  if [[ ! -d "$DIST_APP_BUNDLE" ]]; then
    return
  fi

  mkdir -p "$STAGING_ROOT"
  mkdir -p "$INSTALL_DIR"
  rm -rf "$STAGING_APP_BUNDLE"
  ditto "$DIST_APP_BUNDLE" "$STAGING_APP_BUNDLE"
  resign_bundle "$STAGING_APP_BUNDLE"

  rm -rf "$INSTALL_APP_BUNDLE"
  ditto "$STAGING_APP_BUNDLE" "$INSTALL_APP_BUNDLE"
  resign_bundle "$INSTALL_APP_BUNDLE"
}

prepare_dist_target() {
  if [[ -L "$DIST_APP_BUNDLE" ]]; then
    rm "$DIST_APP_BUNDLE"
  fi
}

prepare_dist_target

if [[ -x "$ROOT_DIR/.venv/bin/pyinstaller" ]]; then
  run_pyinstaller "$ROOT_DIR/.venv/bin/pyinstaller"
elif [[ -x "$ROOT_DIR/.venv_gui/bin/pyinstaller" ]]; then
  run_pyinstaller "$ROOT_DIR/.venv_gui/bin/pyinstaller"
elif [[ -x "$ROOT_DIR/.venv_gui/bin/pyside6-deploy" ]]; then
  "$ROOT_DIR/.venv_gui/bin/pyside6-deploy" "$ROOT_DIR/scripts/desktop_entry.py" -f
elif [[ -x "$ROOT_DIR/.venv/bin/pyside6-deploy" ]]; then
  "$ROOT_DIR/.venv/bin/pyside6-deploy" "$ROOT_DIR/scripts/desktop_entry.py" -f
elif command -v pyside6-deploy >/dev/null 2>&1; then
  pyside6-deploy "$ROOT_DIR/scripts/desktop_entry.py" -f
elif command -v pyinstaller >/dev/null 2>&1; then
  run_pyinstaller "$(command -v pyinstaller)"
else
  pyinstaller --clean --noconfirm "$SPEC_PATH"
fi

APP_BUNDLE="$DIST_APP_BUNDLE"
QTWEBENGINE_FRAMEWORK="$APP_BUNDLE/Contents/Frameworks/PySide6/Qt/lib/QtWebEngineCore.framework"
HELPERS_SOURCE_DIR="$QTWEBENGINE_FRAMEWORK/Versions/Resources/Helpers"
HELPERS_TARGET_LINK="$QTWEBENGINE_FRAMEWORK/Versions/A/Helpers"

if [[ -d "$HELPERS_SOURCE_DIR/QtWebEngineProcess.app" && ! -e "$HELPERS_TARGET_LINK" ]]; then
  ln -s ../Resources/Helpers "$HELPERS_TARGET_LINK"
fi

resign_bundle "$APP_BUNDLE"
install_launchable_copy

echo "Build complete! Launchable app: $INSTALL_APP_BUNDLE"
echo "Build complete! dist bundle: $DIST_APP_BUNDLE"
