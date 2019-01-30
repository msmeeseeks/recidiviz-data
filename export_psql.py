"""Writes a script to export the database to JSON for BigQuery ingestion."""

import sqlalchemy

from recidiviz.persistence.database import schema

BQ_DATASET = 'recidiviz-123:census'

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
    schema.Charge,
    schema.TxCountyAggregate,
]

TABLES_TO_EXPORT = [table.__table__ for table in TABLES_TO_EXPORT]

ALL_TABLE_COLUMNS = {
    table.name: [column.name for column in table.columns]
    for table in TABLES_TO_EXPORT
}

COLUMNS_TO_EXCLUDE = {
    'person': ['full_name']
}

TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(set(ALL_TABLE_COLUMNS.get(table_name))
                     - set(COLUMNS_TO_EXCLUDE.get(table_name, [])))
    for table_name in ALL_TABLE_COLUMNS
}

DATA_FILENAME = '{table}_export.json'

TABLE_EXPORT_QUERY = (
    '\\copy (SELECT ROW_TO_JSON(row) FROM '
    '(SELECT {columns} FROM {table}) row) TO ' + DATA_FILENAME
)

TABLE_EXPORT_QUERIES = {
    table: TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in TABLE_COLUMNS_TO_EXPORT.items()
}

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

BQ_LOAD_COMMAND = (
    'bq load --replace --source_format=NEWLINE_DELIMITED_JSON '
    '{dataset}.{table} {data_source} {schema_source}'
)

SCHEMA_FILENAME = '{table}_schema.json'

BQ_LOAD_COMMANDS = {
    table: BQ_LOAD_COMMAND.format(
        dataset=BQ_DATASET,
        table=table,
        data_source=DATA_FILENAME.format(table=table),
        schema_source=SCHEMA_FILENAME.format(table=table))
    for table in TABLE_EXPORT_SCHEMA
}

PSQL_EXPORT_COMMAND = (
    'psql "sslmode=verify-ca sslrootcert=$CERTDIR/server-ca.pem '
    'sslcert=$CERTDIR/client-cert.pem sslkey=$CERTDIR/client-key.pem '
    'host=$DB_HOST user=$DB_USER dbname=$DB_NAME" '
    '--file={sql_file}'
)

# Dev database does not use client-server certificate checks.
PSQL_EXPORT_COMMAND_DEV = (
    'psql "sslmode=require host=$DB_HOST user=$DB_USER dbname=$DB_NAME" '
    '--file={sql_file}'
)

if __name__ == '__main__':
    import json

    SQL_FILENAME = 'export_all.sql'

    with open(SQL_FILENAME, 'w') as output_file:
        output_file.write(';\n'.join(TABLE_EXPORT_QUERIES.values()))

    for table, schema in TABLE_EXPORT_SCHEMA.items():
        with open(SCHEMA_FILENAME.format(table=table), 'w') as output_file:
            output_file.write(json.dumps(schema))

    with open('psql_export.sh', 'w') as output_file:
        output_file.write(PSQL_EXPORT_COMMAND.format(sql_file=SQL_FILENAME))

    with open('bq_load.sh', 'w') as output_file:
        output_file.write('\n'.join(BQ_LOAD_COMMANDS.values()))
