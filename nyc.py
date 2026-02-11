from __future__ import annotations

from pathlib import Path
import zipfile
import requests
import pandas as pd


URL = "https://s3.amazonaws.com/tripdata/2014-citibike-tripdata.zip"
ZIP_PATH = Path("data/2014-citibike-tripdata.zip")
OUT_CSV = Path("data/citibike_2014_combined.csv")

# Columns we’ll normalize everything into (kept in this order)
CANON_ORDER = [
    "started_at", "ended_at", "trip_duration_sec",
    "start_station_id", "start_station_name", "start_lat", "start_lng",
    "end_station_id", "end_station_name", "end_lat", "end_lng",
    "bike_id", "user_type", "birth_year", "gender",
    "source_file",
]

# Common Citi Bike header variants -> canonical names
RENAME_MAP = {
    # time
    "starttime": "started_at",
    "start time": "started_at",
    "started_at": "started_at",
    "stoptime": "ended_at",
    "stop time": "ended_at",
    "ended_at": "ended_at",

    # duration
    "tripduration": "trip_duration_sec",
    "trip duration": "trip_duration_sec",
    "duration": "trip_duration_sec",

    # station ids/names
    "start station id": "start_station_id",
    "start_station_id": "start_station_id",
    "start station name": "start_station_name",
    "start_station_name": "start_station_name",
    "end station id": "end_station_id",
    "end_station_id": "end_station_id",
    "end station name": "end_station_name",
    "end_station_name": "end_station_name",

    # lat/lng
    "start station latitude": "start_lat",
    "start_lat": "start_lat",
    "start station longitude": "start_lng",
    "start_lng": "start_lng",
    "end station latitude": "end_lat",
    "end_lat": "end_lat",
    "end station longitude": "end_lng",
    "end_lng": "end_lng",

    # bike/user
    "bikeid": "bike_id",
    "bike id": "bike_id",
    "bike_id": "bike_id",
    "usertype": "user_type",
    "user type": "user_type",
    "member_casual": "user_type",  # later schema; keep as user_type
    "birth year": "birth_year",
    "birth_year": "birth_year",
    "gender": "gender",
}


def download_file(url: str, dest: Path, chunk_size: int = 1024 * 1024) -> Path:
    """Stream-download a file to disk (avoids loading big downloads into RAM)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    return dest


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a Citi Bike dataframe from various header schemas into CANON_ORDER.
    Adds any missing canonical columns as NA.
    """
    # Trim and normalize header whitespace; lowercase for matching
    cleaned = {c: " ".join(str(c).strip().split()).lower() for c in df.columns}
    df = df.rename(columns=cleaned)

    # Apply rename map for known variants
    df = df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})

    # Ensure all canonical columns exist
    for col in CANON_ORDER:
        if col not in df.columns:
            df[col] = pd.NA

    # Basic type hygiene
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")
    df["trip_duration_sec"] = pd.to_numeric(df["trip_duration_sec"], errors="coerce")

    # Keep these as strings (they can contain missing values / mixed types)
    for c in ["start_station_id", "end_station_id", "bike_id", "birth_year", "gender", "user_type"]:
        df[c] = df[c].astype("string")

    return df[CANON_ORDER]


def iter_csv_chunks_with_encoding_fallback(file_obj, chunksize: int):
    """
    Read CSV in chunks with encoding fallback.
    Citi Bike older files sometimes aren’t UTF-8 (latin-1/cp1252-like).
    """
    try:
        file_obj.seek(0)
        yield from pd.read_csv(file_obj, chunksize=chunksize)
        return
    except UnicodeDecodeError:
        pass

    # Fallback
    file_obj.seek(0)
    yield from pd.read_csv(file_obj, chunksize=chunksize, encoding="latin-1")


def combine_zip_to_csv(zip_path: Path, out_csv: Path, chunksize: int = 250_000) -> None:
    """
    Combines all CSVs anywhere inside a zip (including subdirectories) into one CSV,
    normalizing columns and writing incrementally.
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    first_write = True

    with zipfile.ZipFile(zip_path) as z:
        members = sorted(
            name for name in z.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        )

        if not members:
            raise RuntimeError("No CSV files found inside the zip.")

        numfiles = 0
        for member in members:
            numfiles = numfiles + 1
            if numfiles > 2:
                break
            with z.open(member) as f:
                for chunk in iter_csv_chunks_with_encoding_fallback(f, chunksize=chunksize):
                    chunk["source_file"] = member
                    chunk = normalize_columns(chunk)

                    chunk.to_csv(
                        out_csv,
                        mode="w" if first_write else "a",
                        index=False,
                        header=first_write,
                    )
                    first_write = False

    print(f"Wrote combined CSV: {out_csv}")


def open_combined_csv(path: Path) -> pd.DataFrame:
    """Reads the combined CSV back in with the canonical columns and parsed datetimes."""
    return pd.read_csv(
        path,
        usecols=CANON_ORDER,
        parse_dates=["started_at", "ended_at"],
        dtype={
            "start_station_id": "string",
            "end_station_id": "string",
            "bike_id": "string",
            "user_type": "string",
            "birth_year": "string",
            "gender": "string",
            "source_file": "string",
        },
    )


if __name__ == "__main__":
    if not ZIP_PATH.exists():
        print(f"Downloading {URL} -> {ZIP_PATH}")
        download_file(URL, ZIP_PATH)

    print("combining csv files")
    combine_zip_to_csv(ZIP_PATH, OUT_CSV, chunksize=250_000)

    print("opening combined csv file")
    df = open_combined_csv(OUT_CSV)
    print("Combined shape:", df.shape)
    print(df.head())
