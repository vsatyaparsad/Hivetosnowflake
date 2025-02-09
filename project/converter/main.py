import sys
import json
import sqlglot
from sqlglot import exp, parse_one
from sqlglot.dialects import hive, snowflake
from typing import List, Dict, Optional, Union

class HiveToSnowflakeConverter:
    def __init__(self):
        self.warnings = []
        
    def convert(self, hive_sql: str) -> dict:
        """Convert Hive SQL to Snowflake SQL using SQLGlot."""
        try:
            # Split into statements
            statements = sqlglot.parse(hive_sql, read='hive')
            
            # Convert each statement
            converted_statements = []
            for stmt in statements:
                # Apply custom transformations
                stmt = self._transform_statement(stmt)
                # Convert to Snowflake
                snowflake_sql = stmt.sql(dialect='snowflake')
                converted_statements.append(snowflake_sql)
            
            # Join statements
            final_sql = ';\n\n'.join(converted_statements)
            
            return {
                "success": True,
                "sql": final_sql,
                "warnings": self.warnings
            }
            
        except Exception as e:
            return {
                "success": False,
                "errors": [str(e)],
                "warnings": self.warnings
            }

    def _transform_statement(self, stmt: exp.Expression) -> exp.Expression:
        """Apply custom transformations to the AST."""
        if isinstance(stmt, exp.Create):
            return self._transform_create(stmt)
        elif isinstance(stmt, exp.Insert):
            return self._transform_insert(stmt)
        elif isinstance(stmt, exp.Select):
            return self._transform_select(stmt)
        return stmt

    def _transform_create(self, stmt: exp.Create) -> exp.Create:
        """Transform CREATE TABLE statements."""
        # Remove EXTERNAL keyword
        stmt.external = False
        
        # Handle partitioning
        if stmt.partitioned_by:
            self.warnings.append("Converting PARTITIONED BY to CLUSTER BY")
            cluster_cols = [col.name for col in stmt.partitioned_by]
            stmt.cluster_by = cluster_cols
            stmt.partitioned_by = None
        
        # Handle storage format
        if hasattr(stmt, 'stored_as'):
            self.warnings.append(f"Removing STORED AS {stmt.stored_as}")
            stmt.stored_as = None
        
        # Remove row format
        if hasattr(stmt, 'row_format'):
            self.warnings.append("Removing ROW FORMAT specification")
            stmt.row_format = None
        
        return stmt

    def _transform_insert(self, stmt: exp.Insert) -> exp.Insert:
        """Transform INSERT statements."""
        # Convert INSERT OVERWRITE to INSERT INTO
        if stmt.overwrite:
            self.warnings.append("Converting INSERT OVERWRITE to INSERT INTO")
            stmt.overwrite = False
        
        # Remove partition specification
        if hasattr(stmt, 'partition'):
            self.warnings.append("Removing partition specification from INSERT")
            stmt.partition = None
        
        return stmt

    def _transform_select(self, stmt: exp.Select) -> exp.Select:
        """Transform SELECT statements."""
        # Convert LATERAL VIEW EXPLODE
        if any(isinstance(t, exp.Lateral) for t in stmt.joins or []):
            self.warnings.append("Converting LATERAL VIEW EXPLODE to FLATTEN")
            new_joins = []
            for join in stmt.joins:
                if isinstance(join, exp.Lateral):
                    new_join = exp.Join(
                        join.this,
                        join.expression,
                        join_type="CROSS",
                        alias=join.alias
                    )
                    new_joins.append(new_join)
                else:
                    new_joins.append(join)
            stmt.joins = new_joins
        
        # Convert window functions
        self._transform_windows(stmt)
        
        return stmt

    def _transform_windows(self, stmt: exp.Select) -> None:
        """Transform window functions in SELECT statement."""
        def transform_window(node):
            if isinstance(node, exp.Window):
                # Convert RANGE to ROWS
                if node.range:
                    self.warnings.append("Converting RANGE to ROWS in window function")
                    node.range = False
                    node.rows = True
            return node

        stmt.transform(transform_window)

def main():
    try:
        # Read input from stdin
        hive_sql = sys.stdin.read()
        
        # Convert the SQL
        converter = HiveToSnowflakeConverter()
        result = converter.convert(hive_sql)
        
        # Output the result as JSON
        print(json.dumps(result))
        
    except Exception as e:
        print(json.dumps({
            "success": False,
            "errors": [str(e)]
        }))

if __name__ == "__main__":
    main()