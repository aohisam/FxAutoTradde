#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ICON_PATH="$ROOT_DIR/resources/app_icon.icns"
SOURCE_ICON_PATH="$ROOT_DIR/icon.png"
SPEC_PATH="$ROOT_DIR/FXAutoTradeLab.spec"

ensure_macos_icon() {
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
  for python_cmd in "$ROOT_DIR/.venv_gui/bin/python" "$ROOT_DIR/.venv/bin/python" python3; do
    if [[ -x "$python_cmd" ]] || command -v "$python_cmd" >/dev/null 2>&1; then
      "$python_cmd" - <<'PY' && return
from pathlib import Path
from PIL import Image

source = Path("icon.png")
target = Path("resources/app_icon.icns")
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
  PYINSTALLER_CONFIG_DIR="$ROOT_DIR/.pyinstaller" \
    "$pyinstaller_cmd" \
      --clean \
      --noconfirm \
      "$SPEC_PATH"
}

if [[ -x "$ROOT_DIR/.venv_gui/bin/pyinstaller" ]]; then
  run_pyinstaller "$ROOT_DIR/.venv_gui/bin/pyinstaller"
elif [[ -x "$ROOT_DIR/.venv/bin/pyinstaller" ]]; then
  run_pyinstaller "$ROOT_DIR/.venv/bin/pyinstaller"
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

APP_BUNDLE="$ROOT_DIR/dist/FXAutoTradeLab.app"
QTWEBENGINE_FRAMEWORK="$APP_BUNDLE/Contents/Frameworks/PySide6/Qt/lib/QtWebEngineCore.framework"
HELPERS_SOURCE_DIR="$QTWEBENGINE_FRAMEWORK/Versions/Resources/Helpers"
HELPERS_TARGET_LINK="$QTWEBENGINE_FRAMEWORK/Versions/A/Helpers"

if [[ -d "$HELPERS_SOURCE_DIR/QtWebEngineProcess.app" && ! -e "$HELPERS_TARGET_LINK" ]]; then
  ln -s ../Resources/Helpers "$HELPERS_TARGET_LINK"
fi

if [[ -d "$APP_BUNDLE" ]] && command -v codesign >/dev/null 2>&1; then
  xattr -cr "$APP_BUNDLE" >/dev/null 2>&1 || true
  codesign --force --deep -s - "$APP_BUNDLE" >/dev/null 2>&1 || true
fi
