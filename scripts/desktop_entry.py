from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fxautotrade_lab.desktop.app import launch_desktop_app


if __name__ == "__main__":
    launch_desktop_app()
