from __future__ import annotations

import os
from typing import Any, Dict, List, Protocol

import yaml
from google.cloud import bigquery
from google.cloud.bigquery.table import Table


class SchemaError(Exception):
    pass


class BigQueryClient(Protocol):
    def get_table(self, _table: str) -> Table:  # noqa: U101
        pass


TableSchema = List[Dict[str, Any]]
DatasetSchema = Dict[str, TableSchema]
ProjectSchema = Dict[str, DatasetSchema]


class SchemaManager:
    """
    SchemaManager manage schema of tables in emulator.

    params:
        schema_file_path:
            path to yaml file which contains schema of tables in emulator.

        client:
            client of production BigQuery. If you set this, SchemaManager will fetch schema from production BigQuery.
    """

    def __init__(self, schema_file_path: str | None = "bqem_master_schema.yaml", client: BigQueryClient | None = None):
        self.__client = client
        self.__schema_file_path = schema_file_path
        self.__change_flg = False

        master_schema: dict[str, ProjectSchema] = {}
        if schema_file_path is not None and os.path.isfile(schema_file_path):
            with open(schema_file_path) as f:
                master_schema = yaml.safe_load(f) or {}

        self.__master_schema: dict[str, ProjectSchema] = master_schema

    def get_schema(self, project_id: str, dataset_id: str, table_id: str) -> list[bigquery.SchemaField]:
        schema = self.__master_schema.get(project_id, {}).get(dataset_id, {}).get(table_id, [])

        if len(schema) == 0:
            result = self._get_schema_from_production(project_id, dataset_id, table_id)
            deepupdate(self.__master_schema, {project_id: {dataset_id: {table_id: [s._properties for s in result]}}})
            self.__change_flg = True
            return result
        else:
            return [bigquery.SchemaField.from_api_repr(s) for s in schema]

    def _get_schema_from_production(self, project_id: str, dataset_id: str, table_id: str) -> list[bigquery.SchemaField]:
        if not self.__client:
            raise SchemaError("client is not set")
        table = self.__client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return table.schema

    def save(self):
        if self.__schema_file_path is not None and self.__change_flg:
            with open(self.__schema_file_path, "w") as f:
                head_msg = '# This code is generated from BigQuery table metadata by "bqemulatormanager"; DO NOT EDIT.\n'
                f.write(head_msg)

                yaml.dump(self.__master_schema, f, encoding="utf8", allow_unicode=True)

    def __del__(self):
        self.save()


def deepupdate(dict_base, other):
    for k, v in other.items():
        if isinstance(v, dict) and k in dict_base:
            deepupdate(dict_base[k], v)
        else:
            dict_base[k] = v
