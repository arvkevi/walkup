import json
import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def __len__(self):
        return len(self.rows)

    def to_csv(self, path, index=False):
        headers = list(self.rows[0].keys())
        lines = [",".join(headers)]
        lines.extend(",".join(str(row[header]) for header in headers) for row in self.rows)
        Path(path).write_text("\n".join(lines) + "\n")


class ExportToKaggleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.original_cwd = os.getcwd()
        self.addCleanup(os.chdir, self.original_cwd)
        os.chdir(self.temp_dir.name)

    def run_export(self, extra_env=None):
        env = {
            "DATABASE_URL": "postgresql://example",
            "KAGGLE_USERNAME": "auth-user",
            "KAGGLE_SLUG": "daily-mlb-walkup-songs",
        }
        if extra_env:
            env.update(extra_env)

        fake_pandas = SimpleNamespace(
            read_sql=mock.Mock(
                return_value=FakeDataFrame(
                    [{"team": "astros", "player": "Jose Altuve", "song_name": "Levels"}]
                )
            )
        )
        fake_sqlalchemy = SimpleNamespace(create_engine=mock.Mock(return_value=object()))

        with mock.patch.dict(os.environ, env, clear=True), mock.patch.dict(
            sys.modules, {"pandas": fake_pandas, "sqlalchemy": fake_sqlalchemy}
        ):
            sys.modules.pop("export_to_kaggle", None)
            export_to_kaggle = importlib.import_module("export_to_kaggle")
            self.addCleanup(sys.modules.pop, "export_to_kaggle", None)
            self.assertEqual(export_to_kaggle.main(), 0)

        return json.loads(Path("kaggle_export/dataset-metadata.json").read_text())

    def test_defaults_dataset_owner_to_auth_username(self):
        metadata = self.run_export()
        self.assertEqual(metadata["id"], "auth-user/daily-mlb-walkup-songs")

    def test_allows_dataset_owner_override(self):
        metadata = self.run_export({"KAGGLE_DATASET_OWNER": "dataset-owner"})
        self.assertEqual(metadata["id"], "dataset-owner/daily-mlb-walkup-songs")


if __name__ == "__main__":
    unittest.main()
