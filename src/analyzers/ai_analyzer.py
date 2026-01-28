"""
AI-powered analysis integration
"""
import json
import logging
from typing import Dict, Any, List, Optional

from ..utils.gemini_client import GeminiClient
from ..utils.pandas_executor import PandasExecutor
from ..utils.vega_generator import VegaChartGenerator
from ..utils.s3_uploader import S3ReportUploader
from ..models import ReportData, ProcessingResult

logger = logging.getLogger(__name__)


class AIAnalyzer:
    """Integrates AI analysis with data processing pipeline"""
    
    def __init__(self, gemini_client: Optional[GeminiClient] = None):
        """
        Initialize AI Analyzer with Gemini client and Pandas executor
        
        Args:
            gemini_client: Pre-configured Gemini client (optional)
        """
        self.gemini_client = gemini_client or GeminiClient()
        self.pandas_executor = PandasExecutor()
        self.vega_generator = VegaChartGenerator()
        self.s3_uploader = S3ReportUploader()
        logger.info("AI analyzer with S3 upload workflow initialized successfully")
    
    def analyze_processing_result(self, result: ProcessingResult, analysis_type: str = "comprehensive") -> ProcessingResult:
        """
        Analyze processing result with AI and add insights
        
        Args:
            result: Processing result containing metadata
            analysis_type: Type of analysis to perform
            
        Returns:
            ProcessingResult with AI insights added
        """
        if not result.success or not result.metadata:
            logger.warning("Cannot perform AI analysis - no metadata available or processing failed")
            return result
        
        try:
            logger.info(f"Starting two-step AI analysis: {analysis_type}")
            
            # Step 1: Start AI session and generate aggregation plan
            ai_response = self.gemini_client.start_analysis_session(
                metadata=result.metadata,
                user_query=f"Generate {analysis_type} aggregations for this restaurant data"
            )
            
            if not ai_response['success']:
                logger.error(f"AI aggregation plan generation failed: {ai_response.get('error', 'Unknown error')}")
                result.add_warning(f"AI plan generation failed: {ai_response.get('error', 'Unknown error')}")
                return result
            
            aggregation_plan = ai_response['aggregation_plan']
            logger.info("Successfully generated aggregation plan")
            logger.info(f"📋 AI Plan Summary: {len(aggregation_plan) if isinstance(aggregation_plan, dict) else 'N/A'} elements")
            
            # Step 2: Execute the aggregation plan with Pandas
            dataframes = {}
            for report in result.reports:
                dataframes[report.name] = report.dataframe
            
            execution_response = self.pandas_executor.execute_aggregation_plan(
                plan=aggregation_plan,
                dataframes=dataframes
            )
            
            if not execution_response['success']:
                logger.error(f"Aggregation execution failed: {execution_response.get('error', 'Unknown error')}")
                result.add_warning(f"Aggregation execution failed: {execution_response.get('error', 'Unknown error')}")
                # Still add the plan even if execution failed
                result.insights.update({
                    'ai_aggregation_plan': aggregation_plan,
                    'execution_error': execution_response.get('error'),
                    'analysis_type': analysis_type
                })
                return result
            
            execution_results = execution_response['results']
            logger.info(f"Successfully executed {len(execution_results)} aggregations")
            logger.info("📊 PANDAS EXECUTION RESULTS:")
            logger.info("-" * 40)
            for key, exec_result in execution_results.items():
                if isinstance(exec_result, dict) and 'summary' in exec_result:
                    summary = exec_result['summary']
                    logger.info(f"  {key}: {summary.get('rows', 0)} rows, operation: {summary.get('operation', 'unknown')}")
            logger.info("-" * 40)
            
            # Step 3: Generate insights from execution results using same AI session
            insights_response = self.gemini_client.generate_insights_from_results(execution_results)
            
            if insights_response['success']:
                # Step 4: Generate visualization charts using same AI session
                logger.info("Starting Step 4: Chart generation with S3 data sources")
                chart_response = self.vega_generator.generate_chart_specs(
                    aggregation_results=execution_results,
                    metadata=result.metadata,
                    gemini_client=self.gemini_client,
                    s3_uploader=self.s3_uploader
                )
                
                # Add comprehensive AI analysis to result
                analysis_result = {
                    'ai_aggregation_plan': aggregation_plan,
                    'aggregation_results': execution_results,
                    'ai_insights': insights_response['insights'],
                    'execution_metadata': execution_response['execution_metadata'],
                    'analysis_metadata': {
                        'plan_generation': ai_response['metadata'],
                        'insights_generation': insights_response['metadata']
                    },
                    'analysis_type': analysis_type
                }
                
                # Add chart specifications if generated successfully
                if chart_response['success']:
                    analysis_result.update({
                        'chart_specifications': chart_response['chart_specs'],
                        'csv_sources': chart_response['csv_sources'],
                        'chart_metadata': chart_response['metadata']
                    })
                    logger.info("✅ Step 4: Chart specifications generated successfully")
                    
                    # Generate HTML dashboard
                    try:
                        chart_specs = chart_response['chart_specs']
                        if isinstance(chart_specs, list):
                            html_dashboard = self.vega_generator.generate_html_dashboard(
                                chart_specs, 
                                title="Restaurant Analytics Dashboard"
                            )
                            
                            # Step 5: Upload to S3 and get shareable links
                            logger.info("Starting Step 5: S3 upload")
                            s3_response = self._upload_to_s3(
                                insights_response['insights'],
                                html_dashboard,
                                chart_response['csv_sources']
                            )
                            
                            if s3_response['success']:
                                analysis_result.update({
                                    's3_upload': s3_response,
                                    'shareable_links': s3_response['urls']
                                })
                                logger.info("✅ Step 5: S3 upload completed successfully")
                            else:
                                logger.warning(f"S3 upload failed: {s3_response.get('error')}")
                                analysis_result['s3_error'] = s3_response.get('error')
                                # Still include local HTML as fallback
                                analysis_result['html_dashboard'] = html_dashboard
                            
                            logger.info("🌐 HTML dashboard generated successfully")
                    except Exception as e:
                        logger.warning(f"HTML dashboard generation failed: {e}")
                        analysis_result['dashboard_error'] = str(e)
                else:
                    logger.warning(f"Chart generation failed: {chart_response.get('error')}")
                    analysis_result['chart_error'] = chart_response.get('error')
                
                result.insights.update(analysis_result)
                logger.info("Successfully completed multi-step AI analysis with visualizations")
                
                # Log AI performance metrics
                plan_tokens = ai_response['metadata'].get('total_tokens', 0)
                insights_tokens = insights_response['metadata'].get('total_tokens', 0)
                chart_tokens = chart_response.get('metadata', {}).get('total_tokens', 0) if chart_response['success'] else 0
                total_tokens = plan_tokens + insights_tokens + chart_tokens
                
                logger.info("🎯 AI PERFORMANCE METRICS:")
                logger.info(f"  • Plan Generation: {plan_tokens} tokens")
                logger.info(f"  • Insights Generation: {insights_tokens} tokens")
                logger.info(f"  • Chart Generation: {chart_tokens} tokens")
                logger.info(f"  • Total AI Usage: {total_tokens} tokens")
                logger.info(f"  • Session Turns: {insights_response['metadata'].get('session_turns', 0)}")
                logger.info(f"  • Charts Generated: {chart_response.get('chart_count', 0) if chart_response['success'] else 0}")
                if 's3_upload' in analysis_result and analysis_result['s3_upload']['success']:
                    logger.info(f"  • S3 Files Uploaded: {len(analysis_result['s3_upload']['urls'])}")
                    logger.info(f"  • S3 Request ID: {analysis_result['s3_upload']['request_id']}")
                logger.info(f"  • Analysis Type: {analysis_type}")
            else:
                logger.error(f"AI insights generation failed: {insights_response.get('error', 'Unknown error')}")
                # Still add plan and execution results
                result.insights.update({
                    'ai_aggregation_plan': aggregation_plan,
                    'aggregation_results': execution_results,
                    'insights_error': insights_response.get('error'),
                    'execution_metadata': execution_response['execution_metadata'],
                    'analysis_type': analysis_type
                })
                result.add_warning(f"AI insights generation failed: {insights_response.get('error', 'Unknown error')}")
        
        except Exception as e:
            logger.error(f"Error during AI analysis: {str(e)}")
            result.add_warning(f"AI analysis error: {str(e)}")
        
        return result
    
    def _upload_to_s3(self, ai_insights: Dict[str, Any], html_dashboard: str, csv_sources: Dict[str, Any]) -> Dict[str, Any]:
        """Upload AI insights and dashboard to S3"""
        try:
            # Extract markdown content from AI insights
            markdown_content = self._extract_markdown_content(ai_insights)
            
            # Upload to S3
            upload_result = self.s3_uploader.upload_report_files(
                markdown_content=markdown_content,
                html_dashboard=html_dashboard,
                csv_sources=csv_sources
            )
            
            return upload_result
            
        except Exception as e:
            logger.error(f"Error in S3 upload: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _extract_markdown_content(self, ai_insights: Dict[str, Any]) -> str:
        """Extract markdown content from AI insights response"""
        # Try different possible locations for the markdown content
        if isinstance(ai_insights, dict):
            # Check for full_response (contains the markdown)
            if 'full_response' in ai_insights:
                return ai_insights['full_response']
            # Check for direct markdown content
            elif 'summary' in ai_insights:
                return ai_insights['summary']
            # Fallback to JSON representation
            else:
                return f"# Restaurant Analysis Report\n\n```json\n{json.dumps(ai_insights, indent=2)}\n```"
        else:
            # If it's a string, assume it's already markdown
            return str(ai_insights)
    
    def analyze_reports_batch(self, reports: List[ReportData], analysis_types: List[str] = None) -> Dict[str, Any]:
        """
        Analyze multiple reports with different analysis types
        
        Args:
            reports: List of report data
            analysis_types: List of analysis types to perform
            
        Returns:
            Dict containing all analysis results
        """
        if analysis_types is None:
            analysis_types = ["comprehensive"]
        
        batch_results = {}
        
        for analysis_type in analysis_types:
            try:
                # Combine metadata from all reports
                combined_metadata = self._combine_reports_metadata(reports)
                
                # Generate aggregation plan and execute
                ai_response = self.gemini_client.start_analysis_session(
                    metadata=combined_metadata,
                    user_query=f"Generate {analysis_type} aggregations and insights for multiple reports"
                )
                
                batch_results[analysis_type] = ai_response
                
            except Exception as e:
                logger.error(f"Error in batch analysis for {analysis_type}: {str(e)}")
                batch_results[analysis_type] = {
                    'success': False,
                    'error': str(e)
                }
        
        return batch_results
    
    def _combine_reports_metadata(self, reports: List[ReportData]) -> Dict[str, Any]:
        """Combine metadata from multiple reports"""
        tables = {}
        
        for report in reports:
            # Create metadata for each report
            df = report.dataframe
            table_name = report.name.split('.')[0]  # Remove extension
            
            tables[table_name] = {
                'file_name': report.name,
                'table_name': table_name,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': [
                    {
                        'column_name': col,
                        'data_type': str(df[col].dtype),
                        'nullable': bool(df[col].isnull().any()),
                        'unique_count': int(df[col].nunique()),
                        'sample_values': df[col].dropna().unique()[:5].tolist()
                    }
                    for col in df.columns
                ],
                'sample_data': {
                    'head': df.head().to_dict(orient='records'),
                    'tail': df.tail().to_dict(orient='records')
                }
            }
        
        return {'tables': tables}
    
    def test_ai_connection(self) -> Dict[str, Any]:
        """Test AI client connection"""
        return self.gemini_client.test_connection()
