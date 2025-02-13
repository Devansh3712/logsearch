import mmap
from functools import wraps
from time import perf_counter
from typing import Callable


def is_start_of_line(file: mmap.mmap, position: int) -> bool:
    if position == 0:
        return True
    file.seek(position - 1)
    return file.read(1) == b"\n"


def get_next_line_position(file: mmap.mmap, position: int) -> int:
    # Read current line till the end
    file.seek(position)
    file.readline()
    return file.tell()


def execution_time(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        func(*args, **kwargs)
        end = perf_counter()
        print(f"took {end - start:.4f}s")

    return wrapper
