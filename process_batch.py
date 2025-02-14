import logging
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
from typing import Dict, List, Optional
import re
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

class SQLFileProcessor:
    def __init__(self, snowflake_config: Dict):
        self.logger = logging.getLogger(__name__)
        self.snowflake_config = snowflake_config
        self.conn = None
        self.variables = {}  # Store variable values
        
    def connect_to_snowflake(self):
        """Establish Snowflake connection"""
        try:
            self.conn = snowflake.connector.connect(
                user=self.snowflake_config['user'],
                password=self.snowflake_config['password'],
                account=self.snowflake_config['account'],
                warehouse=self.snowflake_config['warehouse'],
                database=self.snowflake_config['database'],
                schema=self.snowflake_config['schema']
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def process_file(self, input_file: Path) -> Dict:
        """Process single SQL file with enhanced error reporting"""
        try:
            sql_content = input_file.read_text(encoding='utf-8')
            statements = self._split_statements(sql_content)
            execution_results = []
            
            if self.conn:
                cursor = self.conn.cursor()
                
                # First pass: Process variable declarations
                for stmt in statements:
                    if self._is_variable_declaration(stmt):
                        try:
                            var_name, var_query = self._parse_variable_declaration(stmt)
                            cursor.execute(var_query)
                            result = cursor.fetchone()
                            if result:
                                self.variables[var_name] = result[0]
                                execution_results.append({
                                    "type": "variable_declaration",
                                    "status": "success",
                                    "variable": var_name,
                                    "value": str(result[0]),
                                    "statement": stmt
                                })
                        except Exception as e:
                            execution_results.append({
                                "type": "variable_declaration",
                                "status": "error",
                                "variable": var_name if 'var_name' in locals() else "Unknown",
                                "error": str(e),
                                "statement": stmt,
                                "error_type": type(e).__name__,
                                "error_details": {
                                    "line_number": self._get_error_line(e),
                                    "error_position": self._get_error_position(e),
                                    "suggested_fix": self._suggest_fix(e)
                                }
                            })
                
                # Second pass: Execute main statements
                for stmt in statements:
                    if not self._is_variable_declaration(stmt):
                        try:
                            processed_stmt = self._substitute_variables(stmt)
                            cursor.execute(processed_stmt)
                            execution_results.append({
                                "type": "statement",
                                "status": "success",
                                "rows_affected": cursor.rowcount,
                                "statement": stmt
                            })
                        except Exception as e:
                            execution_results.append({
                                "type": "statement",
                                "status": "error",
                                "error": str(e),
                                "statement": stmt,
                                "error_type": type(e).__name__,
                                "error_details": {
                                    "line_number": self._get_error_line(e),
                                    "error_position": self._get_error_position(e),
                                    "suggested_fix": self._suggest_fix(e)
                                }
                            })
                
                cursor.close()
            
            return {
                "status": "success",
                "variables": self.variables,
                "statements": len(statements),
                "execution_results": execution_results
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "error_details": {
                    "line_number": self._get_error_line(e),
                    "error_position": self._get_error_position(e),
                    "suggested_fix": self._suggest_fix(e)
                }
            }

    def _parse_variable_declaration(self, stmt: str) -> tuple[str, str]:
        """Parse variable declaration statement"""
        # Remove 'SET' and split on first '='
        stmt = stmt.strip()
        if stmt.endswith(';'):
            stmt = stmt[:-1]
        
        parts = stmt.split('=', 1)
        if len(parts) != 2:
            raise ValueError("Invalid variable declaration")
        
        var_name = parts[0].replace('SET', '').strip()
        var_query = parts[1].strip()
        
        # Remove outer parentheses if present
        if var_query.startswith('(') and var_query.endswith(')'):
            var_query = var_query[1:-1].strip()
        
        return var_name, var_query

    def _substitute_variables(self, stmt: str) -> str:
        """Replace variables in SQL statement with their values"""
        processed_stmt = stmt
        for var_name, var_value in self.variables.items():
            # Handle different variable syntaxes
            patterns = [
                f"${var_name}",
                f"${{var_name}}",
                f":{var_name}",
                f"@{var_name}"
            ]
            
            for pattern in patterns:
                if isinstance(var_value, str):
                    # Quote string values
                    processed_stmt = processed_stmt.replace(pattern, f"'{var_value}'")
                else:
                    # Use raw value for numbers, etc.
                    processed_stmt = processed_stmt.replace(pattern, str(var_value))
        
        return processed_stmt

    def _is_variable_declaration(self, stmt: str) -> bool:
        """Check if statement is a variable declaration"""
        stmt = stmt.strip().upper()
        return stmt.startswith('SET') and '=' in stmt

    def _split_statements(self, sql: str) -> List[str]:
        """Split SQL into individual statements"""
        statements = []
        current_stmt = []
        in_quote = False
        quote_char = None
        
        for char in sql:
            if char in ["'", '"']:
                if not in_quote:
                    in_quote = True
                    quote_char = char
                elif char == quote_char:
                    in_quote = False
                    quote_char = None
            
            if char == ';' and not in_quote:
                current_stmt.append(char)
                stmt = ''.join(current_stmt).strip()
                if stmt:
                    statements.append(stmt)
                current_stmt = []
            else:
                current_stmt.append(char)
        
        final_stmt = ''.join(current_stmt).strip()
        if final_stmt:
            statements.append(final_stmt)
        
        return statements

    def _get_error_line(self, error: Exception) -> Optional[int]:
        """Extract line number from error message"""
        try:
            if hasattr(error, 'lineno'):
                return error.lineno
            # Try to extract line number from error message
            msg = str(error)
            line_match = re.search(r'line\s+(\d+)', msg, re.IGNORECASE)
            if line_match:
                return int(line_match.group(1))
        except:
            pass
        return None

    def _get_error_position(self, error: Exception) -> Optional[int]:
        """Extract position from error message"""
        try:
            if hasattr(error, 'position'):
                return error.position
            msg = str(error)
            pos_match = re.search(r'position\s+(\d+)', msg, re.IGNORECASE)
            if pos_match:
                return int(pos_match.group(1))
        except:
            pass
        return None

    def _suggest_fix(self, error: Exception) -> Optional[str]:
        """Suggest possible fixes based on error type"""
        error_msg = str(error).lower()
        
        if 'column not found' in error_msg:
            return "Check column name and table schema"
        elif 'table not found' in error_msg:
            return "Verify table name and database/schema"
        elif 'syntax error' in error_msg:
            return "Check SQL syntax near the error position"
        elif 'permission denied' in error_msg:
            return "Verify user permissions for this operation"
        elif 'duplicate key value' in error_msg:
            return "Check for unique constraint violations"
        elif 'division by zero' in error_msg:
            return "Check for zero values in division operations"
        
        return None

    def _get_sql_context(self, sql: str, error_position: Optional[int], context_lines: int = 3) -> str:
        """Get SQL context around error position"""
        if not error_position:
            return ""
        
        lines = sql.split('\n')
        total_pos = 0
        error_line = 0
        
        # Find the line containing the error
        for i, line in enumerate(lines):
            total_pos += len(line) + 1  # +1 for newline
            if total_pos >= error_position:
                error_line = i
                break
        
        # Get context lines
        start_line = max(0, error_line - context_lines)
        end_line = min(len(lines), error_line + context_lines + 1)
        
        # Build context string
        context = []
        for i in range(start_line, end_line):
            prefix = "  "
            if i == error_line:
                prefix = "→ "  # Arrow pointing to error line
            context.append(f"{prefix}{i+1:4d} | {lines[i]}")
            
            # Add error position marker
            if i == error_line:
                pos_in_line = error_position - sum(len(l) + 1 for l in lines[:i])
                context.append("       " + " " * pos_in_line + "^")
        
        return "\n".join(context)

def create_excel_report(results: Dict[str, Dict], output_file: str = "sql_analysis_report.xlsx"):
    """Create detailed Excel report"""
    report_data = []
    
    for file_name, result in results.items():
        status = result.get("status", "unknown")
        error_msg = result.get("error", "") if status == "error" else ""
        num_statements = result.get("statements", 0) if status == "success" else 0
        variables = len(result.get("variables", {})) if status == "success" else 0
        
        report_data.append({
            "File Name": file_name,
            "Status": status.upper(),
            "Number of Statements": num_statements,
            "Number of Variables": variables,
            "Error Message": error_msg,
            "Processing Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    df = pd.DataFrame(report_data)
    
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='SQL Analysis Results', index=False)
        
        workbook = writer.book
        worksheet = writer.sheets['SQL Analysis Results']
        
        # Formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })
        
        success_format = workbook.add_format({
            'bg_color': '#90EE90'
        })
        
        error_format = workbook.add_format({
            'bg_color': '#FFB6C1'
        })
        
        # Apply formats
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Column widths
        worksheet.set_column('A:A', 40)  # File Name
        worksheet.set_column('B:B', 15)  # Status
        worksheet.set_column('C:D', 20)  # Numbers
        worksheet.set_column('E:E', 50)  # Error Message
        worksheet.set_column('F:F', 20)  # Processing Time
        
        # Row formatting
        for row_num in range(1, len(df) + 1):
            status = df.iloc[row_num-1]['Status']
            if status == 'SUCCESS':
                worksheet.set_row(row_num, None, success_format)
            elif status == 'ERROR':
                worksheet.set_row(row_num, None, error_format)

def create_detailed_error_report(results: Dict[str, Dict], output_file: str = "error_report.txt"):
    """Create detailed error report with SQL context"""
    with open(output_file, "w") as f:
        f.write("SQL Execution Error Report\n")
        f.write("=" * 80 + "\n\n")
        
        for file_name, result in results.items():
            if result["status"] == "error" or any(r["status"] == "error" 
                for r in result.get("execution_results", [])):
                
                f.write(f"File: {file_name}\n")
                f.write("-" * 80 + "\n")
                
                # File level error
                if result["status"] == "error":
                    error = result.get("error", "Unknown error")
                    error_type = result.get("error_type", "Unknown")
                    error_details = result.get("error_details", {})
                    
                    f.write(f"Error Type: {error_type}\n")
                    f.write(f"Error Message: {error}\n")
                    if error_details.get("line_number"):
                        f.write(f"Line Number: {error_details['line_number']}\n")
                    if error_details.get("suggested_fix"):
                        f.write(f"Suggested Fix: {error_details['suggested_fix']}\n")
                    f.write("\n")
                    continue
                
                # Statement level errors
                execution_results = result.get("execution_results", [])
                
                # Variable declaration errors
                var_errors = [r for r in execution_results 
                            if r["type"] == "variable_declaration" and r["status"] == "error"]
                if var_errors:
                    f.write("Variable Declaration Errors:\n")
                    for err in var_errors:
                        f.write(f"\n  • Variable: {err.get('variable', 'Unknown')}\n")
                        f.write(f"    Error Type: {err.get('error_type', 'Unknown')}\n")
                        f.write(f"    Error Message: {err.get('error', 'Unknown error')}\n")
                        
                        # Add SQL context
                        if 'statement' in err:
                            f.write("\n    SQL Context:\n")
                            context = self._get_sql_context(
                                err['statement'],
                                err.get('error_details', {}).get('error_position')
                            )
                            f.write(f"{context}\n")
                        
                        if err.get('error_details', {}).get('suggested_fix'):
                            f.write(f"    Suggested Fix: {err['error_details']['suggested_fix']}\n")
                    f.write("\n")
                
                # Statement execution errors
                stmt_errors = [r for r in execution_results 
                             if r["type"] == "statement" and r["status"] == "error"]
                if stmt_errors:
                    f.write("Statement Execution Errors:\n")
                    for i, err in enumerate(stmt_errors, 1):
                        f.write(f"\n  {i}. Error Type: {err.get('error_type', 'Unknown')}\n")
                        f.write(f"     Error Message: {err.get('error', 'Unknown error')}\n")
                        
                        # Add SQL context
                        if 'statement' in err:
                            f.write("\n     SQL Context:\n")
                            context = self._get_sql_context(
                                err['statement'],
                                err.get('error_details', {}).get('error_position')
                            )
                            for line in context.split('\n'):
                                f.write(f"     {line}\n")
                        
                        if err.get('error_details', {}).get('suggested_fix'):
                            f.write(f"\n     Suggested Fix: {err['error_details']['suggested_fix']}\n")
                    f.write("\n")
                
                f.write("\n")

def main():
    """Process SQL files in batch"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sql_analysis.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Snowflake configuration
    snowflake_config = {
        'user': 'YOUR_USER',
        'password': 'YOUR_PASSWORD',
        'account': 'YOUR_ACCOUNT',
        'warehouse': 'YOUR_WAREHOUSE',
        'database': 'YOUR_DATABASE',
        'schema': 'YOUR_SCHEMA'
    }
    
    try:
        # Initialize processor with Snowflake config
        processor = SQLFileProcessor(snowflake_config)
        
        # Connect to Snowflake
        if not processor.connect_to_snowflake():
            logger.error("Failed to connect to Snowflake. Exiting.")
            return 1
        
        # Get SQL files from snowflake_queries folder
        input_dir = Path("snowflake_queries")
        if not input_dir.exists():
            logger.error(f"Directory not found: {input_dir}")
            return 1
            
        sql_files = list(input_dir.glob('*.sql'))
        
        if not sql_files:
            logger.error(f"No SQL files found in {input_dir}")
            return 1
        
        # Sort files to ensure consistent processing order
        sql_files.sort()
        
        # Process files
        results = {}
        logger.info(f"Starting to process {len(sql_files)} SQL files from {input_dir}")
        
        for sql_file in sql_files:
            logger.info(f"\nProcessing {sql_file.name}")
            print(f"\nProcessing: {sql_file.name}")
            try:
                result = processor.process_file(sql_file)
                results[sql_file.name] = result
                
                # Print immediate status with details
                status = result["status"].upper()
                if status == "SUCCESS":
                    execution_results = result.get("execution_results", [])
                    var_declarations = [r for r in execution_results 
                                     if r["type"] == "variable_declaration"]
                    statements = [r for r in execution_results 
                                if r["type"] == "statement"]
                    
                    # Print variable declarations
                    if var_declarations:
                        print("Variables:")
                        for var in var_declarations:
                            if var["status"] == "success":
                                print(f"  ✓ {var['variable']} = {var['value']}")
                            else:
                                print(f"  ✗ {var['variable']}: {var['error']}")
                    
                    # Print statement execution status
                    failed_stmts = [s for s in statements if s["status"] == "error"]
                    if not failed_stmts:
                        print(f"✅ {sql_file.name}: SUCCESS - All statements executed")
                    else:
                        print(f"⚠️ {sql_file.name}: PARTIAL SUCCESS")
                        print(f"  {len(failed_stmts)} of {len(statements)} statements failed")
                        for stmt in failed_stmts:
                            print(f"  Error: {stmt['error']}")
                else:
                    error = result.get("error", "Unknown error")
                    print(f"❌ {sql_file.name}: FAILED - {error}")
                    
            except Exception as e:
                logger.error(f"Error processing {sql_file.name}: {e}")
                results[sql_file.name] = {"status": "error", "error": str(e)}
                print(f"❌ {sql_file.name}: FAILED - {str(e)}")
        
        # Close Snowflake connection
        if processor.conn:
            processor.conn.close()
        
        # Generate reports with execution results
        create_excel_report(results)
        
        # Save detailed JSON report
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_files": len(results),
            "successful": sum(1 for r in results.values() if r["status"] == "success"),
            "failed": sum(1 for r in results.values() if r["status"] == "error"),
            "details": results
        }
        
        with open("sql_analysis_report.json", "w") as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print("\nProcessing Summary:")
        print("-" * 80)
        print(f"Total Files: {report['total_files']}")
        print(f"Successful: {report['successful']}")
        print(f"Failed: {report['failed']}")
        print(f"Reports generated: sql_analysis_report.xlsx, sql_analysis_report.json")
        print("-" * 80)
        
        # Create detailed error report
        create_detailed_error_report(results)
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    main() 
