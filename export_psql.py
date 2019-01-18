"""Writes a SQL script to export the database to JSON for BigQuery ingestion."""

import sqlalchemy

from recidiviz.persistence.database import schema


BQ_TYPES = {
    sqlalchemy.Boolean: 'BOOL',
    sqlalchemy.Date: 'DATE',
    sqlalchemy.DateTime: 'DATETIME',
    sqlalchemy.Enum: 'STRING',
    sqlalchemy.Integer: 'INT64',
    sqlalchemy.String: 'STRING',
    sqlalchemy.Text: 'STRING'
}

TABLES_TO_EXPORT = [
    schema.Person,
    schema.Booking,
    schema.Hold,
    schema.Arrest,
    schema.Bond,
    schema.Sentence,
    schema.Charge
]

TABLES_TO_EXPORT = [table.__table__ for table in TABLES_TO_EXPORT]

ALL_TABLE_COLUMNS = {table.name: [column.name for column in table.columns]
                     for table in TABLES_TO_EXPORT}

COLUMNS_TO_EXCLUDE = {
    'person': ['full_name'],
    'arrest': ['date']
}

TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(set(ALL_TABLE_COLUMNS.get(table_name))
                     - set(COLUMNS_TO_EXCLUDE.get(table_name, [])))
    for table_name in ALL_TABLE_COLUMNS
}

TABLE_EXPORT_QUERY = (
    '\\copy (SELECT ROW_TO_JSON(row) FROM '
    '(SELECT {columns} FROM {table}) row) TO {table}_export.json;')

TABLE_EXPORT_QUERIES = [
    TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in TABLE_COLUMNS_TO_EXPORT.items()
]

TABLE_EXPORT_QUERY_STRING = '\n'.join(TABLE_EXPORT_QUERIES)

TABLE_EXPORT_SCHEMA = {
    table.name: [
        {'name': column.name,
         'type': BQ_TYPES[type(column.type)],
         'mode': 'NULLABLE' if column.nullable else 'REQUIRED'}
        for column in table.columns
        if column.name in TABLE_COLUMNS_TO_EXPORT[table.name]
    ]
    for table in TABLES_TO_EXPORT
}

if __name__ == '__main__':
    import json

    with open('export_tables.sql', 'w') as output_file:
        output_file.write(TABLE_EXPORT_QUERY_STRING)

    for table, schema in TABLE_EXPORT_SCHEMA.items():
        with open('{}_schema.json'.format(table), 'w') as output_file:
            output_file.write(json.dumps(schema))
