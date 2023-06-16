import tempfile
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
    def test_get_schema_from_file(self):
        with tempfile.NamedTemporaryFile() as fp:
            schema_file_path = fp.name
            schema_data = """---
    project:
      dataset:
        table:
          - name: id
            type: INTEGER
          - name: name
            type: STRING
            """
            fp.write(schema_data.encode())
            fp.seek(0)

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
        with tempfile.NamedTemporaryFile() as fp:
            table = Table("project.dataset.table")
            table.schema = [
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("name", "STRING"),
            ]
            dummy_client = DummyBigQueryClient(table)

            schema_file_path = fp.name
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
