import argparse
import mmap
import time

from rich.console import Console
from rich.text import Text


console = Console()


def read_file(filename: str, query: str | None = None):
    with open(filename, "r+b") as infile:
        with mmap.mmap(infile.fileno(), length=0, access=mmap.ACCESS_READ) as map:
            line_count = 0
            time_start = time.time()
            while line := map.readline():
                if query and (q := query.encode()) in line:
                    pos_start = line.find(q)
                    pos_end = pos_start + len(query)
                    line = line.decode()
                    text = Text.assemble(
                        line[:pos_start],
                        (line[pos_start:pos_end], "bold red"),
                        line[pos_end:],
                    )
                    console.print(text)
                line_count += 1
            time_end = time.time()
            console.print(
                f"read {line_count} lines in {time_end - time_start:.4f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="name of log file", type=str)
    parser.add_argument("-q", "--query", help="search query", type=str)
    args = parser.parse_args()
    read_file(args.file, args.query)
