"""Ingest Divvy monthly trip files into the RAW zone of the lake.

Divvy publishes one zip per month at https://divvy-tripdata.s3.amazonaws.com/.
This script downloads the months you ask for, unzips them, checks the header,
and lands each CSV in a Hive-style folder so Glue/Athena see year and month
as partitions:

    <raw root>/divvy_trips/ingest_year=2026/ingest_month=07/202607-divvy-tripdata.csv

Usage
    # AWS: land straight in the raw S3 bucket
    python -m divvy_lakehouse.ingest --months 2025-10:2026-09 --dest s3://<raw-bucket>

    # Local: copy the committed sample into ./lake/raw (no internet needed)
    python -m divvy_lakehouse.ingest --from-dir data/sample/raw --dest lake/raw
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import shutil
import urllib.request
import zipfile
from pathlib import Path

from divvy_lakehouse.transform import RAW_COLUMNS

DIVVY_BASE_URL = "https://divvy-tripdata.s3.amazonaws.com"
FILE_RE = re.compile(r"(\d{4})(\d{2})-divvy-tripdata\.csv$")


def month_range(spec: str) -> list[str]:
    """'2025-10:2026-01' -> ['202510', '202511', '202512', '202601']"""
    start, _, end = spec.partition(":")
    end = end or start
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def check_header(first_line: str, name: str) -> None:
    header = next(csv.reader([first_line]))
    if header != RAW_COLUMNS:
        raise ValueError(f"{name}: unexpected header {header}. Divvy may have changed the schema.")


def raw_key(yyyymm: str) -> str:
    return f"divvy_trips/ingest_year={yyyymm[:4]}/ingest_month={yyyymm[4:]}/{yyyymm}-divvy-tripdata.csv"


def _put(dest: str, key: str, data: bytes) -> str:
    if dest.startswith("s3://"):
        import boto3  # only needed for the AWS path

        bucket, _, prefix = dest[5:].partition("/")
        full_key = f"{prefix.rstrip('/')}/{key}".lstrip("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=full_key, Body=data,
                                      ServerSideEncryption="AES256")
        return f"s3://{bucket}/{full_key}"
    path = Path(dest) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)


def ingest_month(yyyymm: str, dest: str) -> str:
    """Download one month from Divvy, unzip in memory, land it in the raw zone."""
    url = f"{DIVVY_BASE_URL}/{yyyymm}-divvy-tripdata.zip"
    with urllib.request.urlopen(url, timeout=120) as resp:
        archive = zipfile.ZipFile(io.BytesIO(resp.read()))
    # Divvy zips sometimes contain __MACOSX junk or a CSV with the wrong month
    # in its name (202601's zip holds "202501-divvy-tripdata.csv"), so we take
    # the one real CSV and name it by the month we asked for.
    csvs = [n for n in archive.namelist() if n.endswith(".csv") and not n.startswith("__MACOSX")]
    if len(csvs) != 1:
        raise ValueError(f"{url}: expected 1 CSV, found {csvs}")
    data = archive.read(csvs[0])
    check_header(data.split(b"\n", 1)[0].decode("utf-8-sig"), url)
    return _put(dest, raw_key(yyyymm), data)


def ingest_from_dir(src_dir: str, dest: str) -> list[str]:
    """Land local CSVs (e.g. the committed sample) exactly as the download would."""
    landed = []
    for f in sorted(Path(src_dir).glob("*-divvy-tripdata.csv")):
        match = FILE_RE.search(f.name)
        if not match:
            continue
        with f.open(encoding="utf-8-sig") as fh:
            check_header(fh.readline(), f.name)
        key = raw_key(match.group(1) + match.group(2))
        if dest.startswith("s3://"):
            landed.append(_put(dest, key, f.read_bytes()))
        else:
            target = Path(dest) / key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(f, target)
            landed.append(str(target))
    return landed


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--months", help="YYYY-MM or YYYY-MM:YYYY-MM to download from Divvy")
    src.add_argument("--from-dir", help="folder of already-downloaded monthly CSVs")
    p.add_argument("--dest", required=True, help="s3://bucket[/prefix] or a local folder")
    args = p.parse_args()

    if args.months:
        for m in month_range(args.months):
            print("landed", ingest_month(m, args.dest))
    else:
        for path in ingest_from_dir(args.from_dir, args.dest):
            print("landed", path)


if __name__ == "__main__":
    main()
