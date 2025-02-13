import argparse
import mmap
import os
import re
from dataclasses import dataclass
from concurrent.futures import as_completed, Future, ProcessPoolExecutor

from rich.console import Console
from rich.text import Text

from utils import execution_time, get_next_line_position, is_start_of_line


console = Console()


@dataclass
class Chunk:
    file: str
    start: int
    end: int
    query: str | None = None
    regex: str | None = None


def format_line(line: str, start: int, end: int) -> None:
    text = Text.assemble(
        line[:start], (line[start:end], "bold red"), line[end:])
    console.print(text)


def process_line(line: str, query: str | None, regex: str | None):
    if query and (query in line):
        pos_start = line.find(query)
        pos_end = pos_start + len(query)
        format_line(line, pos_start, pos_end)
    if regex and (match := re.search(regex, line)):
        format_line(line, match.start(), match.end())


def process_chunk(chunk: Chunk):
    with open(chunk.file, "r") as infile:
        with mmap.mmap(
            infile.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as map:
            map.seek(chunk.start)
            while line := map.readline():
                chunk.start += len(line)
                if chunk.start > chunk.end:
                    break
                process_line(line.decode(), chunk.query, chunk.regex)


@execution_time
def process_file(filepath: str, query: str | None = None, regex: str | None = None):
    cpu_count = os.cpu_count()
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // cpu_count
    chunks: list[Chunk] = []

    with open(filepath, "r") as infile:
        with mmap.mmap(infile.fileno(), length=0, access=mmap.ACCESS_READ) as map:
            chunk_start = 0
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk_size)
                # Make sure chunk ends at the beginning of the next line
                while not is_start_of_line(map, chunk_end):
                    chunk_end -= 1
                # When line is too long to fit the chunk size
                if chunk_start == chunk_end:
                    chunk_end = get_next_line_position(map, chunk_end)

                chunks.append(Chunk(filepath, chunk_start,
                              chunk_end, query, regex))
                chunk_start = chunk_end

    futures: list[Future] = []
    with ProcessPoolExecutor() as executor:
        for chunk in chunks:
            future = executor.submit(process_chunk, chunk)
            futures.append(future)

    for future in as_completed(futures):
        future.result()


# TODO:
# Add optional argument for storing results in a file
# Search in multiple log files (multiprocessing)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="path of log file", type=str)
    parser.add_argument("-q", "--query", help="search query", type=str)
    parser.add_argument(
        "-r", "--regex", help="search query as regex", type=str)
    args = parser.parse_args()
    process_file(args.file, args.query, args.regex)
