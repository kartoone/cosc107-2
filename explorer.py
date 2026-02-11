from pathlib import Path
import pandas as pd

OUT_CSV = Path("data/citibike_2014_combined.csv")
# Columns we’ll normalize everything into (kept in this order)
CANON_ORDER = [
    "started_at", "ended_at", "trip_duration_sec",
    "start_station_id", "start_station_name", "start_lat", "start_lng",
    "end_station_id", "end_station_name", "end_lat", "end_lng",
    "bike_id", "user_type", "birth_year", "gender",
    "source_file",
]

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
    print("opening combined csv file")
    df = open_combined_csv(OUT_CSV)
    print("Combined shape:", df.shape)
    print(df.head())
    print(df.columns.tolist())
