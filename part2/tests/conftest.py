import pathlib
import sys
import os

PART2_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PART2_DIR) not in sys.path:
    sys.path.insert(0, str(PART2_DIR))

# тесты не зависят отMLflow state
os.environ.setdefault("USE_MLFLOW", "false")

