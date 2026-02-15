import pathlib
import sys

PART2_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PART2_DIR) not in sys.path:
    sys.path.insert(0, str(PART2_DIR))