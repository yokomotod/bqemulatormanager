import os
import shutil
import unittest

import yaml
from google.cloud import bigquery
from google.cloud.bigquery.table import Table

from bqemulatormanager.schema import SchemaError, SchemaManager


class DummyBigQueryClient:
    def __init__(self, table: Table):
        self.__table = table

    def get_table(self, _table: str) -> Table:  # noqa: U101
        return self.__table


class TestSchemaManager(unittest.TestCase):
    test_schema_dir = ".testing"

    def setUp(self) -> None:
        os.mkdir(self.test_schema_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_schema_dir)

    def test_get_schema_from_file(self):
        schema_file_path = os.path.join(self.test_schema_dir, "test_get_schema_from_file.yaml")
        schema_data = """---
project:
  dataset:
    table:
      - name: id
        type: INTEGER
      - name: name
        type: STRING
        """
        with open(schema_file_path, "w") as f:
            f.write(schema_data)

        schema_manager = SchemaManager(schema_file_path=schema_file_path, client=None)
        got = schema_manager.get_schema("project", "dataset", "table")

        expect = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ]
        self.assertEqual(got, expect)

    def test_get_schema_for_empty_schema(self):
        schema_manager = SchemaManager(schema_file_path=None, client=None)

        with self.assertRaises(SchemaError):
            schema_manager.get_schema("project", "dataset", "table")

    def test_save(self):
        table = Table("project.dataset.table")
        table.schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ]
        dummy_client = DummyBigQueryClient(table)

        schema_file_path = os.path.join(self.test_schema_dir, "test_save.yaml")
        schema_manager = SchemaManager(schema_file_path=schema_file_path, client=dummy_client)

        schema_manager.get_schema("project", "dataset", "table")
        schema_manager.save()

        with open(schema_file_path) as f:
            got = yaml.safe_load(f.read())

        expect = {
            "project": {
                "dataset": {
                    "table": [
                        {"mode": "NULLABLE", "name": "id", "type": "INTEGER"},
                        {"mode": "NULLABLE", "name": "name", "type": "STRING"},
                    ],
                }
            }
        }
        self.assertEqual(got, expect)
