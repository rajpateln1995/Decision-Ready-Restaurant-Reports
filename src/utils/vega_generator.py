"""
Vega-Lite chart generation module
"""
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class VegaChartGenerator:
    """Generates Vega-Lite chart specifications and HTML dashboards"""
    
    def __init__(self):
        """Initialize the Vega chart generator"""
        self.chart_specs = []
        self.csv_sources = {}
    
    def generate_chart_specs(self, aggregation_results: Dict[str, Any], metadata: Dict[str, Any], gemini_client, s3_uploader=None) -> Dict[str, Any]:
        """
        Generate Vega-Lite chart specifications using AI
        
        Args:
            aggregation_results: Results from Pandas execution
            metadata: CSV metadata information
            gemini_client: AI client for generating chart specs
            
        Returns:
            Dict containing Vega-Lite chart specifications
        """
        try:
            logger.info("📊 STARTING VEGA-LITE CHART GENERATION")
            logger.info("=" * 50)
            
            # Log input data for monitoring
            logger.info("📋 CHART GENERATION INPUTS:")
            logger.info(f"  • Aggregation Results: {len(aggregation_results)} results")
            logger.info(f"  • Metadata Tables: {list(metadata.get('tables', {}).keys())}")
            
            # Prepare chart context for AI
            chart_context = self._prepare_chart_context(aggregation_results, metadata)
            
            logger.info("🤖 CHART CONTEXT PREPARED:")
            logger.info("-" * 30)
            logger.info(json.dumps(chart_context, indent=2, default=str))
            logger.info("-" * 30)
            
            # Generate chart recommendations using AI
            chart_response = gemini_client.generate_chart_specifications(chart_context)
            
            if chart_response['success']:
                chart_specs = chart_response['chart_specs']
                
                # Log AI-generated chart specifications (before URL replacement)
                logger.info("📈 AI-GENERATED VEGA-LITE SPECS (with placeholders):")
                logger.info("=" * 50)
                logger.info(json.dumps(chart_specs, indent=2, default=str))
                logger.info("=" * 50)
                
                # Extract CSV sources and upload to S3 first
                csv_sources = self._extract_csv_sources(aggregation_results)
                
                # Upload CSV data to S3 and get URLs
                if s3_uploader and csv_sources:
                    logger.info(f"📤 Uploading {len(csv_sources)} CSV sources to S3...")
                    csv_urls = self._upload_csv_data_to_s3(csv_sources, s3_uploader)
                    
                    if csv_urls:
                        # Replace placeholder URLs with actual S3 URLs
                        updated_chart_specs = self._replace_data_urls(chart_specs, csv_urls, aggregation_results)
                        
                        logger.info("📈 UPDATED CHART SPECS (with S3 URLs):")
                        logger.info("=" * 50)
                        logger.info(json.dumps(updated_chart_specs, indent=2, default=str))
                        logger.info("=" * 50)
                        
                        self.chart_specs = updated_chart_specs
                    else:
                        logger.error("❌ CSV upload to S3 failed - no URLs returned")
                        self.chart_specs = chart_specs
                elif not csv_sources:
                    logger.warning("⚠️ No CSV sources available from aggregation results")
                    logger.warning("   Charts will use placeholder URLs (won't work)")
                    self.chart_specs = chart_specs
                else:
                    logger.warning("⚠️ No S3 uploader provided - using placeholder URLs")
                    self.chart_specs = chart_specs
                
                logger.info("✅ Chart specifications with S3 data sources generated successfully")
                return {
                    'success': True,
                    'chart_specs': self.chart_specs,
                    'csv_sources': csv_sources,
                    'chart_count': len(chart_specs) if isinstance(chart_specs, list) else 1,
                    'metadata': chart_response.get('metadata', {})
                }
            else:
                logger.error(f"❌ Chart generation failed: {chart_response.get('error')}")
                return {
                    'success': False,
                    'error': chart_response.get('error'),
                    'chart_specs': [],
                    'csv_sources': self._extract_csv_sources(aggregation_results)
                }
                
        except Exception as e:
            logger.error(f"❌ Error in chart generation: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'chart_specs': [],
                'csv_sources': {}
            }
    
    def _prepare_chart_context(self, aggregation_results: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare context for AI chart generation"""
        
        # Extract data structure information
        data_summary = {}
        for key, result in aggregation_results.items():
            if isinstance(result, dict) and 'data' in result:
                data = result['data']
                if isinstance(data, list) and len(data) > 0:
                    # Analyze the structure of aggregated data
                    sample_row = data[0]
                    data_summary[key] = {
                        'columns': list(sample_row.keys()) if isinstance(sample_row, dict) else [],
                        'row_count': len(data),
                        'sample_data': data[:3],  # First 3 rows for AI context
                        'data_types': self._infer_data_types(data),
                        'operation': result.get('summary', {}).get('operation', 'unknown')
                    }
        
        return {
            'data_summary': data_summary,
            'metadata': metadata,
            'chart_requirements': {
                'target_audience': 'restaurant_managers',
                'business_focus': ['sales', 'revenue', 'performance', 'trends'],
                'chart_types_preferred': ['bar', 'line', 'pie', 'area'],
                'interactivity': 'basic',
                'responsive': True
            }
        }
    
    def _infer_data_types(self, data: List[Dict]) -> Dict[str, str]:
        """Infer data types from sample data"""
        if not data or not isinstance(data[0], dict):
            return {}
        
        type_inference = {}
        sample = data[0]
        
        for column, value in sample.items():
            if isinstance(value, (int, float)):
                type_inference[column] = 'quantitative'
            elif isinstance(value, str):
                # Check if it looks like a date
                if any(date_word in column.lower() for date_word in ['date', 'time', 'created', 'updated']):
                    type_inference[column] = 'temporal'
                else:
                    type_inference[column] = 'nominal'
            else:
                type_inference[column] = 'nominal'
        
        return type_inference
    
    def _extract_csv_sources(self, aggregation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Extract CSV data for S3 storage (separate from chart data embedding)"""
        
        csv_sources = {}
        
        logger.info("🔍 EXTRACTING CSV SOURCES FROM AGGREGATION RESULTS:")
        logger.info(f"   Available aggregation keys: {list(aggregation_results.keys())}")
        
        for key, result in aggregation_results.items():
            logger.info(f"   Processing key: {key}")
            logger.info(f"   Result type: {type(result)}")
            
            if isinstance(result, dict):
                logger.info(f"   Result keys: {list(result.keys())}")
                
                if 'data' in result:
                    data = result['data']
                    logger.info(f"   Data type: {type(data)}, length: {len(data) if isinstance(data, list) else 'N/A'}")
                    
                    if isinstance(data, list) and len(data) > 0:
                        csv_filename = f"{key}_data.csv"
                        columns = list(data[0].keys()) if isinstance(data[0], dict) else []
                        
                        csv_sources[key] = {
                            'filename': csv_filename,
                            'data': data,
                            'rows': len(data),
                            'columns': columns
                        }
                        
                        logger.info(f"   ✅ Created CSV source: {csv_filename}")
                        logger.info(f"   📊 Columns: {columns}")
                        logger.info(f"   📋 Sample data: {data[0] if data else 'None'}")
                    else:
                        logger.warning(f"   ⚠️ No valid data in {key}: {data}")
                else:
                    logger.warning(f"   ⚠️ No 'data' key in {key}")
            else:
                logger.warning(f"   ⚠️ Result {key} is not a dict")
        
        logger.info("📁 FINAL CSV SOURCES FOR S3:")
        if csv_sources:
            for key, source in csv_sources.items():
                logger.info(f"  • {key}: {source['filename']} ({source['rows']} rows, columns: {source['columns']})")
        else:
            logger.error("❌ NO CSV SOURCES EXTRACTED!")
            logger.error("   This will cause chart placeholder URLs to remain unreplaced")
        
        return csv_sources
    
    def _upload_csv_data_to_s3(self, csv_sources: Dict[str, Any], s3_uploader) -> Dict[str, str]:
        """Upload CSV data to S3 and return URLs"""
        
        logger.info("📤 UPLOADING CSV DATA TO S3")
        csv_urls = {}
        
        # Generate unique request ID for this batch
        from datetime import datetime
        import uuid
        timestamp = datetime.now().strftime("%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        request_id = f"chart_{timestamp}_{unique_id}"
        
        date_folder = datetime.now().strftime("%Y-%m-%d")
        
        for key, source in csv_sources.items():
            try:
                # Convert data to CSV format
                csv_content = s3_uploader._convert_to_csv(source['data'])
                
                # Create S3 path for this CSV
                csv_key = f"reports/{date_folder}/{request_id}/data/{source['filename']}"
                
                # Upload to S3
                csv_url = s3_uploader._upload_content(
                    content=csv_content,
                    key=csv_key,
                    content_type="text/csv"
                )
                
                csv_urls[key] = csv_url
                logger.info(f"📊 Uploaded CSV: {source['filename']} → {csv_url}")
                
            except Exception as e:
                logger.error(f"❌ Failed to upload CSV {key}: {e}")
        
        logger.info(f"✅ Uploaded {len(csv_urls)} CSV files to S3")
        return csv_urls
    
    def _replace_data_urls(self, chart_specs: List[Dict], csv_urls: Dict[str, str], aggregation_results: Dict[str, Any]) -> List[Dict]:
        """Replace placeholder URLs with actual S3 CSV URLs"""
        
        logger.info("🔄 REPLACING PLACEHOLDER URLs WITH S3 URLs")
        
        updated_specs = []
        csv_keys = list(csv_urls.keys())
        
        for i, spec in enumerate(chart_specs):
            if isinstance(spec, dict):
                updated_spec = spec.copy()
                
                # Replace placeholder URL with actual S3 URL
                if 'data' in updated_spec and isinstance(updated_spec['data'], dict):
                    if updated_spec['data'].get('url') == 'CSV_DATA_SOURCE_URL':
                        # Use the i-th CSV source, or first one if not enough sources
                        csv_key = csv_keys[i] if i < len(csv_keys) else csv_keys[0] if csv_keys else None
                        
                        if csv_key and csv_key in csv_urls:
                            updated_spec['data']['url'] = csv_urls[csv_key]
                            logger.info(f"   Chart {i+1}: Using CSV {csv_key} → {csv_urls[csv_key][:60]}...")
                        else:
                            logger.warning(f"   Chart {i+1}: No CSV source available")
                
                updated_specs.append(updated_spec)
            else:
                updated_specs.append(spec)
        
        return updated_specs
    
    def generate_html_dashboard(self, chart_specs: List[Dict], title: str = "Restaurant Analytics Dashboard") -> str:
        """
        Generate HTML dashboard with Vega-Lite charts
        
        Args:
            chart_specs: List of Vega-Lite specifications
            title: Dashboard title
            
        Returns:
            HTML string for the dashboard
        """
        logger.info("🌐 GENERATING HTML DASHBOARD")
        logger.info(f"  • Charts: {len(chart_specs)}")
        logger.info(f"  • Title: {title}")
        
        # Basic HTML template with Vega-Lite
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .dashboard-header {{
            text-align: center;
            color: #333;
            margin-bottom: 30px;
        }}
        .chart-container {{
            background: white;
            margin: 20px 0;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .chart-title {{
            font-size: 18px;
            font-weight: bold;
            margin-bottom: 15px;
            color: #333;
        }}
    </style>
</head>
<body>
    <div class="dashboard-header">
        <h1>{title}</h1>
        <p>Interactive charts powered by Vega-Lite</p>
    </div>
    
    <div id="charts">
        <!-- Charts will be inserted here -->
    </div>
    
    <script>
        // Chart specifications
        const chartSpecs = {json.dumps(chart_specs, indent=2)};
        
        // Render each chart
        chartSpecs.forEach((spec, index) => {{
            const chartContainer = document.createElement('div');
            chartContainer.className = 'chart-container';
            chartContainer.innerHTML = `
                <div class="chart-title">${{spec.title || 'Chart ' + (index + 1)}}</div>
                <div id="chart-${{index}}"></div>
            `;
            document.getElementById('charts').appendChild(chartContainer);
            
            // Embed the chart
            vegaEmbed(`#chart-${{index}}`, spec, {{
                actions: false,
                renderer: 'svg'
            }}).catch(console.error);
        }});
    </script>
</body>
</html>
        """
        
        logger.info("✅ HTML dashboard generated successfully")
        return html_template.strip()
    
    def save_csv_files(self, output_dir: str = "./data") -> Dict[str, str]:
        """
        Save CSV files for external data sources
        
        Args:
            output_dir: Directory to save CSV files
            
        Returns:
            Dict mapping source keys to file paths
        """
        import os
        import csv
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        saved_files = {}
        
        for key, source in self.csv_sources.items():
            file_path = os.path.join(output_dir, source['filename'])
            
            # Write CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                if source['data'] and isinstance(source['data'][0], dict):
                    fieldnames = source['columns']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(source['data'])
                    
                    saved_files[key] = file_path
                    logger.info(f"💾 Saved CSV: {file_path} ({len(source['data'])} rows)")
        
        return saved_files
