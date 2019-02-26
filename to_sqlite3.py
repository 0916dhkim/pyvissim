import csv
import sqlite3
from typing import Dict, List


def __to_identifier(string: str) -> str:
    return '"' + string.replace('"', '""') + '"'


def from_att(att_filename: str,
             db: sqlite3.Connection,
             table_name: str,
             custom_col_type_pairs: Dict[str, str]) -> None:
    field_names: List[str] = None
    with open(att_filename) as f:
        # Read header and determine field names.
        for line in f:
            if line.startswith('$VISION') or line.startswith('*'):
                continue
            elif line.startswith('$'):
                field_names = line[line.index(':') + 1:].strip().split(';')
                break

        # Determine column types.
        col_type_pairs: Dict[str, str] = {}
        for field_name in field_names:
            if field_name in custom_col_type_pairs.keys():
                col_type_pairs[field_name] = custom_col_type_pairs[field_name]
            else:
                col_type_pairs[field_name] = 'TEXT'

        # Create table.
        col_type_comma = ', '.join([__to_identifier(k) + ' ' + col_type_pairs[k] for k in col_type_pairs.keys()])
        create_table_query = 'CREATE TABLE ' + __to_identifier(table_name) + '(' + col_type_comma + ')'
        db.execute(create_table_query)

        # Insert data.
        col_comma = ', '.join([__to_identifier(k) for k in col_type_pairs.keys()])
        insert_query = 'INSERT INTO ' + __to_identifier(table_name) + ' (' + col_comma + ') VALUES (' + ','.join(
            ['?'] * len(col_type_pairs)) + ')'
        cr = csv.DictReader(f, fieldnames=field_names, delimiter=';')
        for row in cr:
            db.execute(insert_query, tuple(row.values()))


def from_csv(csv_filename: str,
             db: sqlite3.Connection,
             table_name: str,
             custom_col_type_pairs: Dict[str, str],
             delimiter: str = None) -> None:
    with open(csv_filename) as f:
        # Read header and determine field names.
        header = f.readline().strip()
        if delimiter is None:
            delimiter = ','
        field_names: List[str] = header.split(delimiter)

        # Determine column types.
        col_type_pairs: Dict[str, str] = {}
        for field_name in field_names:
            if field_name in custom_col_type_pairs.keys():
                col_type_pairs[field_name] = custom_col_type_pairs[field_name]
            else:
                col_type_pairs[field_name] = 'TEXT'

        # Create table.
        col_type_comma = ', '.join([__to_identifier(k) + ' ' + col_type_pairs[k] for k in col_type_pairs.keys()])
        create_table_query = 'CREATE TABLE ' + __to_identifier(table_name) + '(' + col_type_comma + ')'
        db.execute(create_table_query)

        # Insert data.
        col_comma = ', '.join([__to_identifier(k) for k in col_type_pairs.keys()])
        insert_query = 'INSERT INTO ' + __to_identifier(table_name) + ' (' + col_comma + ') VALUES (' + ','.join(
            ['?'] * len(col_type_pairs)) + ')'
        cr = csv.DictReader(f, fieldnames=field_names, delimiter=delimiter)
        for row in cr:
            db.execute(insert_query, tuple(row.values()))
