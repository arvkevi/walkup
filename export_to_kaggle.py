#!/usr/bin/env python3
"""
Export the mlb_walk_up_songs table to a Kaggle-ready directory.

Writes:
  kaggle_export/mlb_walk_up_songs.csv
  kaggle_export/dataset-metadata.json

The Kaggle CLI then publishes that directory as a new version of the dataset
(or creates it on first run).

Env vars:
  DATABASE_URL     Postgres connection string
  KAGGLE_USERNAME  Kaggle account that owns the dataset
  KAGGLE_SLUG      Dataset slug (default: daily-mlb-walkup-songs)
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

EXPORT_DIR = Path("kaggle_export")
CSV_NAME = "mlb_walk_up_songs.csv"


def main() -> int:
    db_url = os.environ["DATABASE_URL"]
    username = os.environ["KAGGLE_USERNAME"]
    slug = os.environ.get("KAGGLE_SLUG", "daily-mlb-walkup-songs")

    EXPORT_DIR.mkdir(exist_ok=True)

    engine = create_engine(db_url)
    df = pd.read_sql("SELECT * FROM mlb_walk_up_songs ORDER BY team, player, song_name", engine)
    csv_path = EXPORT_DIR / CSV_NAME
    df.to_csv(csv_path, index=False)
    print(f"Wrote {len(df)} rows to {csv_path}")

    metadata = {
        "title": "Daily MLB Walk-up Songs",
        "id": f"{username}/{slug}",
        "licenses": [{"name": "CC0-1.0"}],
        "resources": [
            {
                "path": CSV_NAME,
                "description": (
                    "Current MLB walk-up songs by team and player, scraped daily from "
                    "MLB.com ballpark music pages and enriched with Spotify metadata."
                ),
            }
        ],
    }
    (EXPORT_DIR / "dataset-metadata.json").write_text(json.dumps(metadata, indent=2))
    print(f"Wrote metadata for {metadata['id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
