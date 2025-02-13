from typing import Dict, Optional, List, Union
import logging
import re
import sqlglot
from sqlglot import exp, parse_one, parse
from sqlglot.expressions import *
from sqlglot.dialects import hive, snowflake
from .exceptions import ConversionError
from .table_mappings import get_table_info, get_snowflake_type, get_snowflake_table_name
from .function_mapper import FunctionMapper
from .type_mapper import TypeMapper

class HiveToSnowflakeConverter:
    def __init__(self):
        self.logger = logging.getLogger('sql_converter.converter')
        self.function_mapper = FunctionMapper()
        self.type_mapper = TypeMapper()
        
    def convert_query(self, hive_sql: str) -> str:
        """Convert Hive SQL to Snowflake SQL"""
        try:
            self.logger.debug("Starting SQL conversion")
            
            # Remove Hive commands
            sql = self._remove_hive_commands(hive_sql)
            
            # Convert table names
            sql = self._convert_table_references(sql)
            
            # Convert functions using function mapper
            sql = self._convert_functions(sql)
            
            # Convert data types using type mapper
            sql = self._convert_data_types(sql)
            
            # Format the final SQL
            sql = self._format_sql(sql)
            
            return sql.strip()
            
        except Exception as e:
            self.logger.error(f"Conversion error: {str(e)}")
            raise ConversionError(f"Failed to convert query: {str(e)}")

    def _convert_functions(self, sql: str) -> str:
        """Convert Hive functions to Snowflake using function mapper"""
        try:
            # Convert timestamp functions
            sql = self.function_mapper.map_function('unix_timestamp')(sql)
            sql = self.function_mapper.map_function('from_unixtime')(sql)
            
            # Convert string functions
            sql = self.function_mapper.map_function('concat_ws')(sql)
            sql = self.function_mapper.map_function('substr')(sql)
            
            # Convert JSON functions
            sql = self.function_mapper.map_function('get_json_object')(sql)
            sql = self.function_mapper.map_function('json_tuple')(sql)
            
            # Convert array functions
            sql = self.function_mapper.map_function('collect_list')(sql)
            sql = self.function_mapper.map_function('collect_set')(sql)
            sql = self.function_mapper.map_function('explode')(sql)
            
            # Convert date functions
            sql = self.function_mapper.map_function('date_add')(sql)
            sql = self.function_mapper.map_function('date_sub')(sql)
            sql = self.function_mapper.map_function('datediff')(sql)
            
            return sql
            
        except Exception as e:
            self.logger.warning(f"Function conversion error: {str(e)}")
            return sql

    def _convert_table_references(self, sql: str) -> str:
        """Convert table names from Hive to Snowflake"""
        patterns = [
            # CREATE TABLE statements
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\${hivevar:\w+}_\w+|\w+)',
             lambda m: f"CREATE OR REPLACE TABLE {get_snowflake_table_name(m.group(1))}"),
            
            # FROM clauses
            (r'FROM\s+(\${hivevar:\w+}_\w+|\w+)\b',
             lambda m: f"FROM {get_snowflake_table_name(m.group(1))}"),
            
            # JOIN clauses
            (r'JOIN\s+(\${hivevar:\w+}_\w+|\w+)\b',
             lambda m: f"JOIN {get_snowflake_table_name(m.group(1))}"),
            
            # INSERT statements
            (r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?(\${hivevar:\w+}_\w+|\w+)',
             lambda m: f"INSERT INTO {get_snowflake_table_name(m.group(1))}")
        ]
        
        for pattern, replacement in patterns:
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        
        return sql

    def _convert_data_types(self, sql: str) -> str:
        """Convert Hive data types to Snowflake using type mapper"""
        def replace_type(match):
            hive_type = match.group(1)
            return self.type_mapper.map_type(hive_type)
        
        # Find and convert column definitions in CREATE TABLE
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+[^(]+\(([^)]+)\)'
        
        def convert_columns(match):
            columns = match.group(1)
            # Convert each column's data type
            converted = re.sub(r'(\w+)\s+([A-Z_]+(?:\([^)]+\))?)', 
                             lambda m: f"{m.group(1)} {self.type_mapper.map_type(m.group(2))}", 
                             columns)
            return f"({converted})"
        
        sql = re.sub(pattern, convert_columns, sql, flags=re.IGNORECASE | re.DOTALL)
        return sql

    def _remove_hive_commands(self, sql: str) -> str:
        """Remove Hive-specific commands and clauses"""
        # Remove SET commands
        sql = re.sub(r'SET\s+[\w\.]+\s*=\s*[^;]+;', '', sql, flags=re.IGNORECASE)
        
        # Remove storage clauses
        clauses = [
            r'\s*STORED\s+AS\s+\w+',
            r'\s*LOCATION\s+\'[^\']+\'',
            r'\s*ROW\s+FORMAT\s+DELIMITED',
            r'\s*FIELDS\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*LINES\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*STORED\s+BY\s+\'[^\']+\'',
            r'\s*WITH\s+SERDEPROPERTIES\s*\([^)]+\)',
            r'\s*TBLPROPERTIES\s*\([^)]+\)',
            r'\s*PARTITIONED\s+BY\s*\([^)]+\)',
            r'\s*CLUSTERED\s+BY\s*\([^)]+\)\s*INTO\s+\d+\s+BUCKETS',
            r'\s*FORMAT\s*=\s*\w+'
        ]
        
        for clause in clauses:
            sql = re.sub(clause, '', sql, flags=re.IGNORECASE)
        
        return sql.strip()

    def _format_sql(self, sql: str) -> str:
        """Format the SQL query"""
        # Add newlines after common SQL keywords
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY',
            'HAVING', 'JOIN', 'UNION', 'INSERT', 'CREATE', 'WITH'
        ]
        
        for keyword in keywords:
            sql = re.sub(f'\\b{keyword}\\b', f'\n{keyword}', sql, flags=re.IGNORECASE)
        
        # Proper indentation
        lines = sql.split('\n')
        formatted_lines = []
        indent_level = 0
        
        for line in lines:
            line = line.strip()
            if line:
                # Decrease indent for closing parentheses
                if line.startswith(')'):
                    indent_level = max(0, indent_level - 1)
                
                # Add indented line
                formatted_lines.append('    ' * indent_level + line)
                
                # Increase indent for opening parentheses
                if line.endswith('('):
                    indent_level += 1
        
        return '\n'.join(formatted_lines)

    def _preprocess_sql(self, sql: str) -> str:
        """Pre-process SQL to handle common parsing issues"""
        # Remove UTF-8 BOM if present
        sql = sql.replace('\ufeff', '')
        
        # Normalize line endings
        sql = sql.replace('\r\n', '\n')
        
        # Fix common syntax issues that cause parsing errors
        fixes = [
            # Fix missing spaces around operators
            (r'([a-zA-Z0-9_])(=|<|>|<=|>=|!=)([a-zA-Z0-9_])', r'\1 \2 \3'),
            
            # Fix missing spaces after commas
            (r',([^\s])', r', \1'),
            
            # Fix multiple spaces
            (r'\s+', ' '),
            
            # Fix spaces around parentheses
            (r'\(\s+', '('),
            (r'\s+\)', ')'),
            
            # Fix common comment issues
            (r'--([^\n])', r'-- \1'),
            
            # Fix missing semicolons between statements
            (r';\s*([A-Za-z])', r';\n\1'),
            
            # Handle special characters in strings
            (r"'([^']*)'", lambda m: f"'{m.group(1).replace(';', '\\;')}'")
        ]
        
        for pattern, replacement in fixes:
            sql = re.sub(pattern, replacement, sql)
        
        return sql.strip()

    def _format_sql(self, sql: str) -> str:
        """Format SQL for better readability"""
        # Split into statements
        statements = sql.split(';')
        formatted_statements = []
        
        for stmt in statements:
            if not stmt.strip():
                continue
                
            # Format each statement
            formatted = self._format_statement(stmt.strip())
            formatted_statements.append(formatted)
        
        # Join statements with proper spacing
        return ';\n\n'.join(formatted_statements)

    def _format_statement(self, stmt: str) -> str:
        """Format a single SQL statement"""
        # Handle comments first
        comments = []
        comment_pattern = r'(--[^\n]*|/\*(?:[^*]|\*(?!/))*\*/)'
        
        def save_comment(match):
            comments.append(match.group(1))
            return f'__COMMENT_{len(comments)-1}__'
        
        # Save comments and replace with placeholders
        stmt = re.sub(comment_pattern, save_comment, stmt, flags=re.DOTALL)
        
        # Format the SQL
        formatted = self._format_sql_core(stmt)
        
        # Restore comments with proper indentation
        for i, comment in enumerate(comments):
            if comment.startswith('--'):
                # Line comments get their own line
                formatted = formatted.replace(
                    f'__COMMENT_{i}__',
                    f'\n{comment}\n'
                )
            else:
                # Block comments get their own paragraph
                formatted = formatted.replace(
                    f'__COMMENT_{i}__',
                    f'\n{comment}\n\n'
                )
        
        # Clean up extra newlines
        formatted = re.sub(r'\n\s*\n', '\n\n', formatted)
        return formatted.strip()

    def _format_sql_core(self, sql: str) -> str:
        """Core SQL formatting logic"""
        # Add newlines after keywords
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
            'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'CROSS JOIN',
            'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'CREATE', 'INSERT',
            'UPDATE', 'DELETE', 'ALTER', 'DROP', 'PARTITION BY', 'CLUSTER BY',
            'DISTRIBUTE BY', 'SORT BY', 'LATERAL VIEW', 'PIVOT', 'UNPIVOT'
        ]
        
        # Format each keyword
        for keyword in keywords:
            sql = re.sub(
                f'\\b{keyword}\\b',
                f'\n{keyword}\n  ',
                sql,
                flags=re.IGNORECASE
            )
        
        # Format window functions
        sql = re.sub(
            r'OVER\s*\(',
            'OVER (\n  ',
            sql,
            flags=re.IGNORECASE
        )
        
        # Format CASE statements
        sql = re.sub(
            r'\bCASE\b',
            '\nCASE\n  ',
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'\bWHEN\b',
            '\n  WHEN',
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'\bTHEN\b',
            ' THEN',
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'\bELSE\b',
            '\n  ELSE',
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'\bEND\b',
            '\nEND',
            sql,
            flags=re.IGNORECASE
        )
        
        # Handle nested queries
        sql = re.sub(r'\(([^\(\)]+)\)', self._format_nested_query, sql)
        
        # Clean up whitespace
        lines = [line.strip() for line in sql.splitlines()]
        lines = [line for line in lines if line]
        
        return self._apply_indentation(lines)

    def _apply_indentation(self, lines):
        """Apply proper indentation to SQL lines"""
        formatted_lines = []
        indent_level = 0
        
        for line in lines:
            # Adjust indent for closing parentheses
            while line.startswith(')'):
                indent_level = max(0, indent_level - 1)
                line = line[1:].strip()
            
            # Add line with proper indent
            if line:
                formatted_lines.append('    ' * indent_level + line)
            
            # Adjust indent for next line
            indent_level += line.count('(') - line.count(')')
            indent_level = max(0, indent_level)
            
            # Additional indent after certain keywords
            if any(line.upper().startswith(kw) for kw in ['SELECT', 'FROM', 'WHERE', 'GROUP BY']):
                indent_level += 1
        
        return '\n'.join(formatted_lines)

    def _format_nested_query(self, match: re.Match) -> str:
        """Format a nested query within parentheses"""
        inner_sql = match.group(1)
        if 'SELECT' in inner_sql.upper():
            return f'(\n{self._format_statement(inner_sql)}\n)'
        return match.group(0)
    
    def _apply_custom_transformations(self, sql: str) -> str:
        """Apply custom transformations for unsupported features"""
        # First convert table names
        sql = self._convert_table_references(sql)
        
        # Then apply other transformations
        sql = self._remove_hive_commands(sql)
        sql = self._convert_dml_statements(sql)
        sql = self._convert_ddl_statements(sql)
        sql = self._convert_functions(sql)
        
        return sql

    def _convert_dml_statements(self, sql: str) -> str:
        """Convert Hive DML statements to Snowflake syntax"""
        # Convert DELETE to TRUNCATE where appropriate
        sql = re.sub(
            r'DELETE\s+FROM\s+(\w+)\s*;?\s*INSERT\s+INTO',
            r'TRUNCATE TABLE \1;\nINSERT INTO',
            sql,
            flags=re.IGNORECASE
        )
        
        # Convert INSERT OVERWRITE to TRUNCATE + INSERT
        sql = re.sub(
            r'INSERT\s+OVERWRITE\s+(?:INTO\s+)?(?:TABLE\s+)?(\w+)',
            r'TRUNCATE TABLE \1;\nINSERT INTO \1',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove TABLE keyword from INSERT
        sql = re.sub(
            r'INSERT\s+INTO\s+TABLE\s+',
            'INSERT INTO ',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove PARTITION clause from INSERT
        sql = re.sub(
            r'\s*PARTITION\s*\([^)]+\)',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
    
    def _convert_ddl_statements(self, sql: str) -> str:
        """Convert Hive DDL statements to Snowflake syntax"""
        # Convert CREATE TABLE statements to CREATE OR REPLACE TABLE
        sql = re.sub(
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'CREATE OR REPLACE TABLE \1',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove FORMAT clause from CREATE TABLE
        sql = re.sub(
            r'\s+FORMAT\s*=\s*\w+',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        # Convert data types
        type_mappings = {
            r'STRING': 'VARCHAR',
            r'TEXT': 'VARCHAR',
            r'INT': 'INTEGER',
            r'BIGINT': 'BIGINT',
            r'DOUBLE': 'DOUBLE',
            r'BOOLEAN': 'BOOLEAN',
            r'BINARY': 'BINARY',
            r'TIMESTAMP': 'TIMESTAMP',
            r'DECIMAL\(([^)]+)\)': r'NUMBER(\1)',
            r'ARRAY<([^>]+)>': r'ARRAY',
            r'MAP<([^,]+),([^>]+)>': r'OBJECT',
            r'STRUCT<([^>]+)>': r'VARIANT'  # Changed from OBJECT to VARIANT
        }
        
        for hive_type, snow_type in type_mappings.items():
            sql = re.sub(
                f'\\b{hive_type}\\b',
                snow_type,
                sql,
                flags=re.IGNORECASE
            )
        
        # Remove Hive-specific storage clauses
        storage_clauses = [
            r'\s*STORED\s+AS\s+\w+',
            r'\s*LOCATION\s+\'[^\']+\'',
            r'\s*ROW\s+FORMAT\s+DELIMITED',
            r'\s*FIELDS\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*LINES\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*STORED\s+BY\s+\'[^\']+\'',
            r'\s*WITH\s+SERDEPROPERTIES\s*\([^)]+\)',
            r'\s*TBLPROPERTIES\s*\([^)]+\)'
        ]
        
        for clause in storage_clauses:
            sql = re.sub(clause, '', sql, flags=re.IGNORECASE)
        
        return sql

    def _convert_create_table(self, match: re.Match) -> str:
        """Convert CREATE TABLE statement using mappings if available"""
        full_match = match.group(0)
        table_name = match.group(1)
        
        # Try to get table info from mappings
        table_info = get_table_info(table_name)
        if not table_info:
            # If no mapping found, convert the original statement
            return self._convert_unmapped_table(full_match)
        
        # Generate CREATE TABLE with mapped columns
        columns = [f"{col.name} {col.snow_type}" for col in table_info.columns]
        
        # Add partition columns if any
        if table_info.partition_columns:
            partition_cols = [f"{col.name} {col.snow_type}" 
                            for col in table_info.partition_columns]
            columns.extend(partition_cols)
        
        create_stmt = f"""CREATE OR REPLACE TABLE {table_info.name} (
    {',\n    '.join(columns)}
)"""
        
        return create_stmt

    def _convert_unmapped_table(self, create_stmt: str) -> str:
        """Convert CREATE TABLE statement without mappings"""
        # Remove EXTERNAL and IF NOT EXISTS
        stmt = re.sub(
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'CREATE OR REPLACE TABLE \1',
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # Convert data types
        def convert_type(match: re.Match) -> str:
            hive_type = match.group(0)
            return get_snowflake_type(hive_type)
        
        # Find and convert column definitions
        def convert_columns(match: re.Match) -> str:
            cols = match.group(1)
            # Convert each column's data type
            converted = re.sub(r'\b[A-Z_]+(?:\([^)]+\))?\b', convert_type, cols)
            return f"({converted})"
        
        stmt = re.sub(r'\((.*?)\)', convert_columns, stmt, flags=re.DOTALL)
        
        # Remove Hive-specific clauses
        stmt = self._remove_hive_clauses(stmt)
        
        return stmt

    def _remove_hive_clauses(self, sql: str) -> str:
        """Remove Hive-specific clauses from CREATE TABLE"""
        clauses = [
            r'\s*STORED\s+AS\s+\w+',
            r'\s*LOCATION\s+\'[^\']+\'',
            r'\s*ROW\s+FORMAT\s+DELIMITED',
            r'\s*FIELDS\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*LINES\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*STORED\s+BY\s+\'[^\']+\'',
            r'\s*WITH\s+SERDEPROPERTIES\s*\([^)]+\)',
            r'\s*TBLPROPERTIES\s*\([^)]+\)',
            r'\s*FORMAT\s*=\s*\w+'
        ]
        
        # Keep track of parentheses
        parts = sql.split(')')
        if len(parts) > 1:
            # Process everything before the last ')'
            main_part = ')'.join(parts[:-1]) + ')'
            # Keep any trailing content after the last ')'
            trailing = parts[-1]
            
            # Remove Hive clauses only from the trailing part
            for clause in clauses:
                trailing = re.sub(f'{clause}.*?(?:;|$)', '', trailing, flags=re.IGNORECASE)
            
            sql = main_part + trailing
        
        return sql.strip()

    def _convert_json_operations(self, sql: str) -> str:
        """Convert Hive JSON operations to Snowflake syntax"""
        # Convert LATERAL VIEW json_tuple
        pattern = r'LATERAL\s+VIEW\s+JSON_TUPLE\(([^,]+),\s*([^)]+)\)\s+(\w+)\s+AS\s+([^;\n]+)'
        
        def replace_json_tuple(match):
            json_col, fields, alias, field_names = match.groups()
            fields = [f.strip().strip("'\"") for f in fields.split(',')]
            field_names = [f.strip() for f in field_names.split(',')]
            
            selects = []
            for field, name in zip(fields, field_names):
                if '.' in field:
                    # Handle nested paths
                    path = field.replace('.', ':')
                    selects.append(f"GET_PATH(PARSE_JSON({json_col}), '{path}')::{name}")
                else:
                    selects.append(f"PARSE_JSON({json_col}):{field}::{name}")
            
            return f"CROSS JOIN LATERAL (SELECT {', '.join(selects)})"
        
        sql = re.sub(pattern, replace_json_tuple, sql, flags=re.IGNORECASE)
        
        # Convert explode variations
        sql = re.sub(
            r'LATERAL\s+VIEW\s+EXPLODE\(([^)]+)\)\s+(\w+)\s+AS\s+(\w+)',
            r'CROSS JOIN LATERAL FLATTEN(input => \1) AS \2(\3)',
            sql,
            flags=re.IGNORECASE
        )
        
        sql = re.sub(
            r'LATERAL\s+VIEW\s+POSEXPLODE\(([^)]+)\)\s+(\w+)\s+AS\s+(\w+),\s*(\w+)',
            r'CROSS JOIN LATERAL FLATTEN(input => \1) AS \2(SEQ, \3, \4)',
            sql,
            flags=re.IGNORECASE
        )
        
        # Convert JSON array functions
        sql = re.sub(
            r'get_json_object\(([^,]+),\s*\'\$\[(\d+)\]\'\)',
            r'GET_PATH(PARSE_JSON(\1), \'[\2]\')',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
    
    def _convert_dml_statements(self, sql: str) -> str:
        """Convert Hive DML statements to Snowflake syntax"""
        # Convert DELETE to TRUNCATE where appropriate
        sql = re.sub(
            r'DELETE\s+FROM\s+(\w+)\s*;?\s*INSERT\s+INTO',
            r'TRUNCATE TABLE \1;\nINSERT INTO',
            sql,
            flags=re.IGNORECASE
        )
        
        # Convert INSERT OVERWRITE to TRUNCATE + INSERT
        sql = re.sub(
            r'INSERT\s+OVERWRITE\s+(?:INTO\s+)?(?:TABLE\s+)?(\w+)',
            r'TRUNCATE TABLE \1;\nINSERT INTO \1',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove TABLE keyword from INSERT
        sql = re.sub(
            r'INSERT\s+INTO\s+TABLE\s+',
            'INSERT INTO ',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove PARTITION clause from INSERT
        sql = re.sub(
            r'\s*PARTITION\s*\([^)]+\)',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
    
    def _convert_ddl_statements(self, sql: str) -> str:
        """Convert Hive DDL statements to Snowflake syntax"""
        # Convert CREATE TABLE statements to CREATE OR REPLACE TABLE
        sql = re.sub(
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'CREATE OR REPLACE TABLE \1',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove FORMAT clause from CREATE TABLE
        sql = re.sub(
            r'\s+FORMAT\s*=\s*\w+',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        # Convert data types
        type_mappings = {
            r'STRING': 'VARCHAR',
            r'TEXT': 'VARCHAR',
            r'INT': 'INTEGER',
            r'BIGINT': 'BIGINT',
            r'DOUBLE': 'DOUBLE',
            r'BOOLEAN': 'BOOLEAN',
            r'BINARY': 'BINARY',
            r'TIMESTAMP': 'TIMESTAMP',
            r'DECIMAL\(([^)]+)\)': r'NUMBER(\1)',
            r'ARRAY<([^>]+)>': r'ARRAY',
            r'MAP<([^,]+),([^>]+)>': r'OBJECT',
            r'STRUCT<([^>]+)>': r'VARIANT'  # Changed from OBJECT to VARIANT
        }
        
        for hive_type, snow_type in type_mappings.items():
            sql = re.sub(
                f'\\b{hive_type}\\b',
                snow_type,
                sql,
                flags=re.IGNORECASE
            )
        
        # Remove Hive-specific storage clauses
        storage_clauses = [
            r'\s*STORED\s+AS\s+\w+',
            r'\s*LOCATION\s+\'[^\']+\'',
            r'\s*ROW\s+FORMAT\s+DELIMITED',
            r'\s*FIELDS\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*LINES\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*STORED\s+BY\s+\'[^\']+\'',
            r'\s*WITH\s+SERDEPROPERTIES\s*\([^)]+\)',
            r'\s*TBLPROPERTIES\s*\([^)]+\)'
        ]
        
        for clause in storage_clauses:
            sql = re.sub(clause, '', sql, flags=re.IGNORECASE)
        
        return sql

    def _convert_create_table(self, match: re.Match) -> str:
        """Convert CREATE TABLE statement using mappings if available"""
        full_match = match.group(0)
        table_name = match.group(1)
        
        # Try to get table info from mappings
        table_info = get_table_info(table_name)
        if not table_info:
            # If no mapping found, convert the original statement
            return self._convert_unmapped_table(full_match)
        
        # Generate CREATE TABLE with mapped columns
        columns = [f"{col.name} {col.snow_type}" for col in table_info.columns]
        
        # Add partition columns if any
        if table_info.partition_columns:
            partition_cols = [f"{col.name} {col.snow_type}" 
                            for col in table_info.partition_columns]
            columns.extend(partition_cols)
        
        create_stmt = f"""CREATE OR REPLACE TABLE {table_info.name} (
    {',\n    '.join(columns)}
)"""
        
        return create_stmt

    def _convert_unmapped_table(self, create_stmt: str) -> str:
        """Convert CREATE TABLE statement without mappings"""
        # Remove EXTERNAL and IF NOT EXISTS
        stmt = re.sub(
            r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'CREATE OR REPLACE TABLE \1',
            create_stmt,
            flags=re.IGNORECASE
        )
        
        # Convert data types
        def convert_type(match: re.Match) -> str:
            hive_type = match.group(0)
            return get_snowflake_type(hive_type)
        
        # Find and convert column definitions
        def convert_columns(match: re.Match) -> str:
            cols = match.group(1)
            # Convert each column's data type
            converted = re.sub(r'\b[A-Z_]+(?:\([^)]+\))?\b', convert_type, cols)
            return f"({converted})"
        
        stmt = re.sub(r'\((.*?)\)', convert_columns, stmt, flags=re.DOTALL)
        
        # Remove Hive-specific clauses
        stmt = self._remove_hive_clauses(stmt)
        
        return stmt

    def _remove_hive_clauses(self, sql: str) -> str:
        """Remove Hive-specific clauses from CREATE TABLE"""
        clauses = [
            r'\s*STORED\s+AS\s+\w+',
            r'\s*LOCATION\s+\'[^\']+\'',
            r'\s*ROW\s+FORMAT\s+DELIMITED',
            r'\s*FIELDS\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*LINES\s+TERMINATED\s+BY\s+\'[^\']+\'',
            r'\s*STORED\s+BY\s+\'[^\']+\'',
            r'\s*WITH\s+SERDEPROPERTIES\s*\([^)]+\)',
            r'\s*TBLPROPERTIES\s*\([^)]+\)',
            r'\s*FORMAT\s*=\s*\w+'
        ]
        
        # Keep track of parentheses
        parts = sql.split(')')
        if len(parts) > 1:
            # Process everything before the last ')'
            main_part = ')'.join(parts[:-1]) + ')'
            # Keep any trailing content after the last ')'
            trailing = parts[-1]
            
            # Remove Hive clauses only from the trailing part
            for clause in clauses:
                trailing = re.sub(f'{clause}.*?(?:;|$)', '', trailing, flags=re.IGNORECASE)
            
            sql = main_part + trailing
        
        return sql.strip()

    def _convert_table_references(self, sql: str) -> str:
        """Convert table names from Hive to Snowflake"""
        # Find and replace table names in different contexts
        patterns = [
            # CREATE TABLE statements
            (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\${hivevar:\w+}_\w+|\w+)',
             lambda m: f"CREATE OR REPLACE TABLE {get_snowflake_table_name(m.group(1))}"),
            
            # FROM clauses
            (r'FROM\s+(\${hivevar:\w+}_\w+|\w+)\b',
             lambda m: f"FROM {get_snowflake_table_name(m.group(1))}"),
            
            # JOIN clauses
            (r'JOIN\s+(\${hivevar:\w+}_\w+|\w+)\b',
             lambda m: f"JOIN {get_snowflake_table_name(m.group(1))}"),
            
            # INSERT statements
            (r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?(\${hivevar:\w+}_\w+|\w+)',
             lambda m: f"INSERT INTO {get_snowflake_table_name(m.group(1))}"),
            
            # TRUNCATE statements
            (r'TRUNCATE\s+TABLE\s+(\${hivevar:\w+}_\w+|\w+)',
             lambda m: f"TRUNCATE TABLE {get_snowflake_table_name(m.group(1))}")
        ]
        
        # Apply each pattern
        for pattern, replacement in patterns:
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        
        return sql

    def _remove_hive_commands(self, sql: str) -> str:
        """Remove Hive-specific commands and hints"""
        # Remove all SET commands
        sql = re.sub(
            r'SET\s+[\w\.]+\s*=\s*[^;]+;',
            '',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove specific Hive SET commands
        hive_settings = [
            'hive.exec.parallel',
            'hive.auto.convert.join',
            'hive.optimize.skewjoin',
            'hive.skewjoin.key',
            'hive.exec.dynamic.partition',
            'hive.exec.dynamic.partition.mode',
            'mapred.reduce.tasks',
            'hive.merge.mapfiles',
            'hive.merge.mapredfiles'
        ]
        
        for setting in hive_settings:
            sql = re.sub(
                f'SET\s+{setting}\s*=\s*[^;]+;',
                '',
                sql,
                flags=re.IGNORECASE
            )
        
        # Remove Hive hints and comments
        hint_patterns = [
            r'/\*\+[^*]*\*/',  # Optimizer hints
            r'--\s*MAPJOIN\([^)]*\)',  # MapJoin hints
            r'--\s*STREAMTABLE\([^)]*\)',  # StreamTable hints
            r'/\*\s*hive\.[^*]*\*/',  # Hive-specific comments
            r'--\s*set\s+hive\.[^;\n]*',  # Hive settings in comments
            r'--\s*distribute\s+by[^;\n]*',  # Distribution hints
            r'--\s*cluster\s+by[^;\n]*',  # Clustering hints
            r'--\s*sort\s+by[^;\n]*'  # Sorting hints
        ]
        
        for pattern in hint_patterns:
            sql = re.sub(pattern, '', sql, flags=re.IGNORECASE)
        
        # Remove empty lines and extra whitespace
        sql = re.sub(r'\n\s*\n', '\n\n', sql)
        sql = re.sub(r';\s*;', ';', sql)
        
        return sql.strip() 
