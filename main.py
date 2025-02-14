import argparse
import mmap
import os
import re
from dataclasses import dataclass
from concurrent.futures import (
    as_completed,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)

from rich.console import Console
from rich.text import Text

from utils import execution_time, get_next_line_position, is_start_of_line


console = Console()


@dataclass
class Chunk:
    file: str
    start: int
    end: int
    query: str | None
    regex: str | None


def format_line(line: str, start: int, end: int) -> None:
    text = Text.assemble(line[:start], (line[start:end], "bold red"), line[end:])
    console.print(text)


def process_line(line: str, query: str | None, regex: str | None, output: bool) -> bool:
    found = False
    start, end = 0, 0
    if query and (query in line):
        found = True
        start = line.find(query)
        end = start + len(query)
    if regex and (match := re.search(regex, line)):
        found = True
        start = match.start()
        end = match.end()
    if found and not output:
        format_line(line, start, end)
    return found


def process_chunk(chunk: Chunk, output: bool) -> tuple[list[str], int]:
    lines: list[str] = []
    count = 0
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
                count += 1
                line = line.decode()
                if process_line(line, chunk.query, chunk.regex, output) and output:
                    lines.append(line)
    return lines, count


@execution_time
def process_file(
    filepath: str, query: str | None, regex: str | None, output: str | None
) -> int:
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

                chunks.append(Chunk(filepath, chunk_start, chunk_end, query, regex))
                chunk_start = chunk_end

    futures: list[Future] = []
    with ThreadPoolExecutor() as executor:
        has_output = output is not None
        for chunk in chunks:
            future = executor.submit(process_chunk, chunk, has_output)
            futures.append(future)

    result: list[str] = []
    total_count = 0
    for future in as_completed(futures):
        lines, count = future.result()
        total_count += count
        if lines:
            result.extend(lines)
    with open(output, "w") as outfile:
        outfile.writelines(result)
    return total_count


def process_files(
    files: list[str], query: str | None, regex: str | None, output: str | None
):
    futures: list[Future] = []
    with ProcessPoolExecutor() as executor:
        for index, file in enumerate(files):
            if output is not None:
                output = f"{index}_{output}"
            future = executor.submit(process_file, file.strip(), query, regex, output)
            futures.append(future)

    for future in futures:
        future.result()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="path of log file", type=str)
    parser.add_argument("-q", "--query", help="search query", type=str)
    parser.add_argument("-r", "--regex", help="search query as regex", type=str)
    parser.add_argument("-o", "--output", help="output file", type=str)
    args = parser.parse_args()

    files = args.file.split(",")
    if len(files) == 1:
        process_file(args.file, args.query, args.regex, args.output)
    else:
        process_files(files, args.query, args.regex, args.output)
