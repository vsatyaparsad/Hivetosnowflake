from typing import Dict, Optional, List, Union
import logging
import re
import sqlglot
from sqlglot import exp, parse_one, parse
from sqlglot.expressions import *
from sqlglot.dialects import hive, snowflake
from .exceptions import ConversionError

class HiveToSnowflakeConverter:
    def __init__(self):
        self.logger = logging.getLogger('sql_converter.converter')
        
    def convert_query(self, hive_sql: str) -> str:
        """Convert Hive SQL to Snowflake SQL using two-step approach"""
        try:
            self.logger.debug("Starting SQL conversion")
            
            # Step 1: Try sqlglot first
            try:
                # Parse and transform using sqlglot
                expressions = parse(hive_sql, read='hive')
                converted = []
                
                for expr in expressions:
                    # Transform using sqlglot's built-in conversion
                    snowflake_sql = expr.sql(dialect='snowflake')
                    converted.append(snowflake_sql)
                
                result = ';\n\n'.join(converted)
                self.logger.debug("sqlglot conversion completed")
                
            except Exception as e:
                self.logger.warning(f"sqlglot conversion failed: {e}")
                result = hive_sql
            
            # Step 2: Apply custom transformations
            result = self._apply_custom_transformations(result)
            
            # Step 3: Format the SQL
            result = self._format_sql(result)
            
            self.logger.debug("Conversion and formatting completed")
            return result.strip()
            
        except Exception as e:
            self.logger.error(f"Conversion error: {str(e)}")
            raise ConversionError(f"Failed to convert query: {str(e)}")

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
        # Remove Hive commands and hints
        sql = self._remove_hive_commands(sql)
        
        # Convert DML statements
        sql = self._convert_dml_statements(sql)
        
        # Convert DDL statements
        sql = self._convert_ddl_statements(sql)
        
        # Convert CREATE TABLE statements
        sql = re.sub(
            r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)',
            r'CREATE OR REPLACE TABLE \1',
            sql,
            flags=re.IGNORECASE
        )
        
        # Remove partitioning and clustering
        sql = re.sub(r'\s*PARTITIONED\s+BY\s*\([^)]+\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s*CLUSTERED\s+BY\s*\([^)]+\)\s+INTO\s+\d+\s+BUCKETS', '', sql, flags=re.IGNORECASE)
        
        # Convert Hive functions
        sql = self._convert_functions(sql)
        
        # Convert JSON operations
        sql = self._convert_json_operations(sql)
        
        # Remove storage format
        sql = re.sub(r'\s*STORED\s+AS\s+\w+', '', sql, flags=re.IGNORECASE)
        
        # Convert session time difference calculation
        sql = re.sub(
            r'STR_TO_UNIX\(([^,]+),\s*[\'"][^\'"]+[\'"]\)\s*-\s*STR_TO_UNIX\(([^,]+),\s*[\'"][^\'"]+[\'"]\)\s*>\s*1800',
            r'DATEDIFF(SECOND, \2, \1) > 1800',
            sql,
            flags=re.IGNORECASE
        )
        
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
    
    def _convert_functions(self, sql: str) -> str:
        """Convert Hive functions to Snowflake equivalents"""
        # Timestamp/Unix conversions
        sql = re.sub(
            r'str_to_unix\(([^,]+),\s*[\'"]([^\'"]+)[\'"]\)',
            r'DATE_PART(EPOCH_SECOND, TO_TIMESTAMP(\1))',
            sql,
            flags=re.IGNORECASE
        )
        
        sql = re.sub(
            r'unix_timestamp\(([^)]+)\)',
            r'DATE_PART(EPOCH_SECOND, \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        sql = re.sub(
            r'from_unixtime\(([^)]+)\)',
            r'DATEADD(SECOND, \1, \'1970-01-01\'::TIMESTAMP)',
            sql,
            flags=re.IGNORECASE
        )
        
        # Date functions
        sql = re.sub(r'date_add\(([^,]+),\s*(\d+)\)', r'DATEADD(DAY, \2, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'date_sub\(([^,]+),\s*(\d+)\)', r'DATEADD(DAY, -\2, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'datediff\(([^,]+),([^)]+)\)', r'DATEDIFF(DAY, \2, \1)', sql, flags=re.IGNORECASE)
        
        # Time extraction
        sql = re.sub(r'year\(([^)]+)\)', r'DATE_PART(YEAR, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'month\(([^)]+)\)', r'DATE_PART(MONTH, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'day\(([^)]+)\)', r'DATE_PART(DAY, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'hour\(([^)]+)\)', r'DATE_PART(HOUR, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'minute\(([^)]+)\)', r'DATE_PART(MINUTE, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'second\(([^)]+)\)', r'DATE_PART(SECOND, \1)', sql, flags=re.IGNORECASE)
        
        # Time arithmetic
        sql = re.sub(
            r'(\w+)\s*-\s*INTERVAL\s+\'(\d+)\'\s+(\w+)',
            r'DATEADD(\3, -\2, \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        sql = re.sub(
            r'(\w+)\s*\+\s*INTERVAL\s+\'(\d+)\'\s+(\w+)',
            r'DATEADD(\3, \2, \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # JSON functions
        sql = re.sub(
            r'get_json_object\(([^,]+),\s*\'\$\.([^\']+)\'\)',
            lambda m: f"GET_PATH(PARSE_JSON({m.group(1)}), '{m.group(2).replace('.', ':')}')",
            sql,
            flags=re.IGNORECASE
        )
        
        # Array functions
        sql = re.sub(r'collect_list\(([^)]+)\)', r'ARRAY_AGG(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'collect_set\(([^)]+)\)', r'ARRAY_AGG(DISTINCT \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'array_contains\(([^,]+),\s*([^)]+)\)', r'ARRAY_CONTAINS(\2, \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'size\(([^)]+)\)', r'ARRAY_SIZE(\1)', sql, flags=re.IGNORECASE)
        
        # String functions
        sql = re.sub(r'concat_ws\(([^,]+),([^)]+)\)', r'ARRAY_TO_STRING(ARRAY_CONSTRUCT(\2), \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'regexp_replace\(([^,]+),([^,]+),([^)]+)\)', r'REGEXP_REPLACE(\1, \2, \3)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'regexp_extract\(([^,]+),([^,]+),(\d+)\)', r'REGEXP_SUBSTR(\1, \2, 1, 1, \'e\', \3)', sql, flags=re.IGNORECASE)
        
        # Aggregate functions
        sql = re.sub(r'percentile\(([^,]+),([^)]+)\)', r'PERCENTILE_CONT(\2) WITHIN GROUP (ORDER BY \1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'percentile_approx\(([^,]+),([^)]+)\)', r'APPROX_PERCENTILE(\1, \2)', sql, flags=re.IGNORECASE)
        
        # Window functions
        sql = re.sub(r'FIRST_VALUE\(([^)]+)\)\s+IGNORE\s+NULLS', r'FIRST_VALUE(\1) IGNORE NULLS', sql, flags=re.IGNORECASE)
        sql = re.sub(r'LAST_VALUE\(([^)]+)\)\s+IGNORE\s+NULLS', r'LAST_VALUE(\1) IGNORE NULLS', sql, flags=re.IGNORECASE)
        
        # Type casting
        sql = re.sub(r'cast\(([^)]+)\s+as\s+string\)', r'TO_VARCHAR(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'cast\(([^)]+)\s+as\s+double\)', r'TO_DOUBLE(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'cast\(([^)]+)\s+as\s+decimal\)', r'TO_DECIMAL(\1)', sql, flags=re.IGNORECASE)
        
        # Map functions
        sql = re.sub(r'str_to_map\(([^)]+)\)', r'OBJECT_CONSTRUCT_FROM_STRING(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'map_keys\(([^)]+)\)', r'OBJECT_KEYS(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'map_values\(([^)]+)\)', r'OBJECT_VALUES(\1)', sql, flags=re.IGNORECASE)
        
        # Conditional functions
        sql = re.sub(r'nvl\(([^,]+),([^)]+)\)', r'COALESCE(\1, \2)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'if\(([^,]+),([^,]+),([^)]+)\)', r'IFF(\1, \2, \3)', sql, flags=re.IGNORECASE)
        
        # Set operations
        sql = re.sub(r'sort_array\(([^)]+)\)', r'ARRAY_SORT(\1)', sql, flags=re.IGNORECASE)
        sql = re.sub(r'array_distinct\(([^)]+)\)', r'ARRAY_UNIQUE(\1)', sql, flags=re.IGNORECASE)
        
        return sql
    
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
