"""
Pandas execution engine for AI-generated aggregation plans
"""
import logging
import pandas as pd
import json
from typing import Dict, Any, List, Optional
from io import StringIO

logger = logging.getLogger(__name__)


class PandasExecutor:
    """Executes AI-generated Pandas aggregation plans"""
    
    def __init__(self):
        """Initialize the Pandas executor"""
        self.dataframes = {}
        self.results = {}
    
    def execute_aggregation_plan(self, plan: Dict[str, Any], dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Execute an AI-generated aggregation plan
        
        Args:
            plan: JSON aggregation plan from AI
            dataframes: Dictionary of DataFrames to operate on
            
        Returns:
            Dict containing execution results and aggregated data
        """
        try:
            logger.info("🔧 STARTING PANDAS EXECUTION")
            logger.info(f"📊 Input DataFrames: {list(dataframes.keys())}")
            logger.info(f"📊 Total Rows to Process: {sum(len(df) for df in dataframes.values())}")
            logger.info("🤖 AI PLAN TO EXECUTE:")
            logger.info("-" * 30)
            logger.info(json.dumps(plan, indent=2, default=str))
            logger.info("-" * 30)
            
            self.dataframes = dataframes
            self.results = {}
            
            # Parse and execute the plan
            execution_results = self._parse_and_execute(plan)
            
            logger.info(f"✅ Successfully executed {len(execution_results)} aggregations")
            
            # Log execution summary
            logger.info("📈 EXECUTION SUMMARY:")
            logger.info(f"  • Tables Processed: {len(dataframes)}")
            logger.info(f"  • Aggregations Executed: {len(execution_results)}")
            logger.info(f"  • Total Rows Processed: {sum(len(df) for df in dataframes.values())}")
            
            return {
                'success': True,
                'results': execution_results,
                'execution_metadata': {
                    'tables_processed': len(dataframes),
                    'aggregations_executed': len(execution_results),
                    'total_rows_processed': sum(len(df) for df in dataframes.values())
                }
            }
            
        except Exception as e:
            logger.error(f"Error executing aggregation plan: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'partial_results': self.results
            }
    
    def _parse_and_execute(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and execute the aggregation plan"""
        results = {}
        
        # Handle different plan formats
        if 'aggregations' in plan:
            # Standard format with multiple aggregations
            for i, agg in enumerate(plan['aggregations']):
                result_key = f"aggregation_{i+1}"
                results[result_key] = self._execute_single_aggregation(agg)
                
        elif 'operations' in plan:
            # Operations-based format
            for i, op in enumerate(plan['operations']):
                result_key = f"operation_{i+1}"
                results[result_key] = self._execute_operation(op)
                
        elif 'tables' in plan:
            # Table-specific format
            for table_name, table_ops in plan['tables'].items():
                if table_name in self.dataframes:
                    results[table_name] = self._execute_table_operations(
                        self.dataframes[table_name], table_ops
                    )
                    
        else:
            # Try to execute as a single aggregation
            results['main_aggregation'] = self._execute_flexible_plan(plan)
            
        return results
    
    def _execute_single_aggregation(self, agg: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single aggregation operation"""
        try:
            # Debug: Check what we actually received
            logger.info(f"🔍 EXECUTING SINGLE AGGREGATION:")
            logger.info(f"   Input type: {type(agg)}")
            logger.info(f"   Input content: {agg}")
            
            # Type checking - ensure agg is a dictionary
            if not isinstance(agg, dict):
                error_msg = f"Expected aggregation to be a dictionary, got {type(agg)}: {agg}"
                logger.error(f"❌ Type error: {error_msg}")
                return {'error': error_msg}
            
            # Get the target DataFrame with smart table name resolution
            requested_table = agg.get('table')
            available_tables = list(self.dataframes.keys())
            logger.info(f"   📋 Available tables: {available_tables}")
            logger.info(f"   🎯 Requested table: {requested_table}")
            
            # Try exact match first
            if requested_table and requested_table in self.dataframes:
                table_name = requested_table
                logger.info(f"   ✅ Using exact match: {table_name}")
            else:
                # Fallback: Use first available table (for now)
                table_name = available_tables[0] if available_tables else None
                if requested_table:
                    logger.warning(f"   ⚠️ Table '{requested_table}' not found, using fallback: {table_name}")
                else:
                    logger.info(f"   📋 No table specified, using first available: {table_name}")
            
            if not table_name or table_name not in self.dataframes:
                raise ValueError(f"No valid table found. Requested: {requested_table}, Available: {available_tables}")
                
            df = self.dataframes[table_name].copy()
            logger.info(f"   DataFrame shape: {df.shape}")
            logger.info(f"   DataFrame columns: {list(df.columns)}")
            
            # Apply filters if specified
            if 'filters' in agg:
                df = self._apply_filters(df, agg['filters'])
            
            # Apply grouping and aggregation
            # Handle both field name formats: AI uses 'groupby'/'agg_functions', code expects 'group_by'/'aggregate'
            group_by_field = agg.get('group_by') or agg.get('groupby')
            aggregate_field = agg.get('aggregate') or agg.get('agg_functions')
            
            if group_by_field and aggregate_field:
                logger.info(f"   📊 Group by: {group_by_field}")
                logger.info(f"   📊 Aggregate: {aggregate_field}")
                result = self._group_and_aggregate(df, group_by_field, aggregate_field)
            elif aggregate_field:
                logger.info(f"   📊 Simple aggregate: {aggregate_field}")
                result = self._simple_aggregate(df, aggregate_field)
            else:
                logger.info("   📊 No aggregation specified, returning full data")
                result = df
            
            # Apply sorting if specified
            if 'sort' in agg:
                sort_config = agg['sort']
                if isinstance(sort_config, list) and len(sort_config) > 0:
                    # Handle list of sort configurations - use the first one
                    result = self._apply_sorting(result, sort_config[0])
                    logger.info(f"   📊 Applied sorting: {sort_config[0]}")
                elif isinstance(sort_config, dict):
                    # Handle single sort configuration  
                    result = self._apply_sorting(result, sort_config)
                    logger.info(f"   📊 Applied sorting: {sort_config}")
                else:
                    logger.warning(f"   ⚠️ Invalid sort config: {sort_config}")
            
            # Apply limit if specified
            if 'limit' in agg:
                result = result.head(agg['limit'])
            
            operation_summary = {
                'data': result.to_dict('records'),
                'summary': {
                    'rows': len(result),
                    'columns': list(result.columns),
                    'operation': agg.get('type', 'aggregation')
                }
            }
            
            # Log operation result
            logger.info(f"  ✓ Operation completed: {operation_summary['summary']['operation']}")
            logger.info(f"    → Result: {operation_summary['summary']['rows']} rows, {len(operation_summary['summary']['columns'])} columns")
            
            return operation_summary
            
        except Exception as e:
            logger.error(f"❌ Error in single aggregation: {str(e)}")
            logger.error(f"   Aggregation input was: {agg}")
            logger.error(f"   Input type was: {type(agg)}")
            return {'error': str(e)}
    
    def _execute_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a general operation"""
        # This can be extended for more complex operations
        return self._execute_single_aggregation(operation)
    
    def _execute_table_operations(self, df: pd.DataFrame, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute multiple operations on a table"""
        results = {}
        current_df = df.copy()
        
        for i, op in enumerate(operations):
            op_result = self._execute_single_aggregation({**op, 'df': current_df})
            results[f"step_{i+1}"] = op_result
            
        return results
    
    def _execute_flexible_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Try to execute a plan with flexible structure"""
        # Use the first available DataFrame
        df = list(self.dataframes.values())[0].copy()
        
        # Look for common aggregation patterns
        result_data = df
        
        # Basic summary if no specific operations
        if len(plan) == 0 or not any(key in plan for key in ['group_by', 'aggregate', 'filters']):
            numeric_cols = df.select_dtypes(include=[float, int]).columns
            if len(numeric_cols) > 0:
                result_data = df[numeric_cols].describe()
            
        return {
            'data': result_data.to_dict('records') if hasattr(result_data, 'to_dict') else str(result_data),
            'summary': {
                'rows': len(result_data) if hasattr(result_data, '__len__') else 1,
                'columns': list(result_data.columns) if hasattr(result_data, 'columns') else [],
                'operation': 'flexible_execution'
            }
        }
    
    def _apply_filters(self, df: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
        """Apply filtering operations"""
        result_df = df.copy()
        
        for filter_op in filters:
            column = filter_op.get('column')
            operator = filter_op.get('operator', '==')
            value = filter_op.get('value')
            
            if column in result_df.columns:
                if operator == '==':
                    result_df = result_df[result_df[column] == value]
                elif operator == '>':
                    result_df = result_df[result_df[column] > value]
                elif operator == '<':
                    result_df = result_df[result_df[column] < value]
                elif operator == '>=':
                    result_df = result_df[result_df[column] >= value]
                elif operator == '<=':
                    result_df = result_df[result_df[column] <= value]
                elif operator == '!=':
                    result_df = result_df[result_df[column] != value]
                elif operator == 'in':
                    result_df = result_df[result_df[column].isin(value)]
                    
        return result_df
    
    def _group_and_aggregate(self, df: pd.DataFrame, group_by: List[str], aggregations: Dict[str, Any]) -> pd.DataFrame:
        """Apply grouping and aggregation"""
        valid_group_cols = [col for col in group_by if col in df.columns]
        
        if not valid_group_cols:
            return df
            
        grouped = df.groupby(valid_group_cols)
        
        # Apply aggregations
        agg_dict = {}
        for col, agg_func in aggregations.items():
            if col in df.columns:
                if isinstance(agg_func, list):
                    agg_dict[col] = agg_func
                else:
                    agg_dict[col] = [agg_func]
        
        if agg_dict:
            result = grouped.agg(agg_dict)
            # Flatten column names if multi-level
            if result.columns.nlevels > 1:
                result.columns = ['_'.join(col).strip() for col in result.columns.values]
            return result.reset_index()
        else:
            return grouped.size().reset_index(name='count')
    
    def _simple_aggregate(self, df: pd.DataFrame, aggregations: Dict[str, Any]) -> pd.DataFrame:
        """Apply simple aggregation without grouping"""
        result_dict = {}
        
        for col, agg_func in aggregations.items():
            if col in df.columns:
                if agg_func == 'sum':
                    result_dict[f"{col}_sum"] = [df[col].sum()]
                elif agg_func == 'mean':
                    result_dict[f"{col}_mean"] = [df[col].mean()]
                elif agg_func == 'count':
                    result_dict[f"{col}_count"] = [df[col].count()]
                elif agg_func == 'max':
                    result_dict[f"{col}_max"] = [df[col].max()]
                elif agg_func == 'min':
                    result_dict[f"{col}_min"] = [df[col].min()]
        
        return pd.DataFrame(result_dict)
    
    def _apply_sorting(self, df: pd.DataFrame, sort_config: Dict[str, Any]) -> pd.DataFrame:
        """Apply sorting to the result"""
        column = sort_config.get('column')
        ascending = sort_config.get('ascending', True)
        
        if column and column in df.columns:
            return df.sort_values(by=column, ascending=ascending)
        
        return df
