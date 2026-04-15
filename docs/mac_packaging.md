# macOS パッケージング

## 推奨

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
./scripts/package_macos_app.sh
```

Anaconda 系 Python で `Qt platform plugin "cocoa"` 初期化に失敗する場合は、Homebrew Python 3.11 で GUI 専用 venv を作成してから次を実行します。

```bash
/opt/homebrew/bin/python3.11 -m venv .venv_gui
./.venv_gui/bin/pip install PySide6 alpaca-py click==8.1.7 numpy pandas plotly pyarrow pydantic pydantic-settings PyYAML typer
./scripts/launch_desktop_macos.sh
```

`.app` 生成後は Finder から起動して Dock に固定できます。起動時には stale process 掃除が走るため、古い `launch-desktop` 系プロセスが残りにくい構成です。

## フォールバック

```bash
pip install pyinstaller
pyinstaller --name FXAutoTradeLab --windowed --noconfirm scripts/desktop_entry.py
```

## 注意

- Apple Silicon arm64 環境で実行
- QWebEngine を使う場合は PySide6 の互換性を確認
- レポート HTML は `.app` 内からも相対参照されないよう外部出力を維持
