import argparse
import mmap
import re
import time

from rich.console import Console
from rich.text import Text


console = Console()


def format_line(line: str, start: int, end: int) -> None:
    text = Text.assemble(
        line[:start], (line[start:end], "bold red"), line[end:])
    console.print(text)


# TODO:
# Figure out how to use multiprocessing with mmap
# Use file chunks using seek
def read_file(
    filename: str, query: str | None = None, regex: str | None = None
) -> None:
    with open(filename, "r+b") as infile:
        with mmap.mmap(infile.fileno(), length=0, access=mmap.ACCESS_READ) as map:
            line_count = 0
            time_start = time.time()
            while line := map.readline().decode():
                if query and (query in line):
                    pos_start = line.find(query)
                    pos_end = pos_start + len(query)
                    format_line(line, pos_start, pos_end)
                if regex and (match := re.search(regex, line)):
                    format_line(line, match.start(), match.end())
                line_count += 1
            time_end = time.time()
            console.print(
                f"read {line_count} lines in {time_end - time_start:.4f}s")


# TODO:
# Add optional argument for storing results in a file
# Search in multiple log files (multiprocessing)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="name of log file", type=str)
    parser.add_argument("-q", "--query", help="search query", type=str)
    parser.add_argument(
        "-r", "--regex", help="search query as regex", type=str)
    args = parser.parse_args()
    read_file(args.file, args.query, args.regex)
