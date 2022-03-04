from pathlib import Path
from shutil import rmtree


def remove_dir_if_exists(dir_path):
    p = Path(dir_path)
    if p.exists():
        rmtree(dir_path)