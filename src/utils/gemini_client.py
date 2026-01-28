"""
Gemini AI client for generating insights from CSV metadata
"""
import json
import logging
import os
from typing import Dict, Any, Optional

import google.generativeai as genai

logger = logging.getLogger(__name__)


class GeminiClient:
    """Client for interacting with Google's Gemini AI API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Gemini client
        
        Args:
            api_key: Google AI API key (hardcoded for now)
        """
        # API key must be provided via environment or parameter
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("Gemini API key must be provided via GEMINI_API_KEY environment variable or parameter")
        if not self.api_key:
            raise ValueError("API key is required for Gemini client")
        
        # Configure the API
        genai.configure(api_key=self.api_key)
        
        # Initialize the model
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Configure generation parameters
        self.generation_config = {
            'temperature': 0.7,
            'top_p': 0.8,
            'top_k': 40,
            'max_output_tokens': 4096,
        }
        
        # Chat session for multi-turn conversations
        self.chat_session = None
    
    def start_analysis_session(self, metadata: Dict[str, Any], user_query: str = "Generate summary insights") -> Dict[str, Any]:
        """
        Start a new analysis session and generate Pandas aggregation plan
        
        Args:
            metadata: Rich metadata extracted from CSV files
            user_query: User request for insights or aggregations
            
        Returns:
            Dict containing Pandas aggregation plan in JSON format
        """
        try:
            # Start a new chat session
            self.chat_session = self.model.start_chat(history=[])
            
            # Build the aggregation prompt
            prompt = self._build_aggregation_prompt(metadata, user_query)
            
            # Generate response using chat session
            logger.info("Starting analysis session and generating aggregation plan")
            response = self.chat_session.send_message(prompt)
            
            # Parse and structure the response
            aggregation_plan = self._parse_response(response.text)
            
            # Log the AI plan for monitoring
            logger.info("🤖 AI AGGREGATION PLAN GENERATED:")
            logger.info("=" * 50)
            logger.info(json.dumps(aggregation_plan, indent=2, default=str))
            logger.info("=" * 50)
            
            logger.info("Successfully generated aggregation plan and started session")
            return {
                'success': True,
                'aggregation_plan': aggregation_plan,
                'session_active': True,
                'metadata': {
                    'model': 'gemini-1.5-flash',
                    'total_tokens': self._estimate_tokens(prompt, response.text),
                    'tables_analyzed': len(metadata.get('tables', {}))
                }
            }
            
        except Exception as e:
            logger.error(f"Error starting analysis session: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'session_active': False
            }
    
    def generate_insights_from_results(self, execution_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from execution results using existing chat session
        
        Args:
            execution_results: Results from executing the aggregation plan
            
        Returns:
            Dict containing AI-generated insights
        """
        try:
            if not self.chat_session:
                raise ValueError("No active chat session. Call start_analysis_session first.")
            
            # Build the insights prompt with execution results
            prompt = self._build_insights_prompt(execution_results)
            
            # Generate insights using the same chat session (maintains context)
            logger.info("Generating insights from execution results")
            response = self.chat_session.send_message(prompt)
            
            # Parse and structure the insights
            insights = self._parse_response(response.text)
            
            # Log the AI summary for monitoring
            logger.info("🧠 AI BUSINESS INSIGHTS GENERATED:")
            logger.info("=" * 50)
            logger.info(json.dumps(insights, indent=2, default=str))
            logger.info("=" * 50)
            
            logger.info("Successfully generated insights from results")
            return {
                'success': True,
                'insights': insights,
                'metadata': {
                    'model': 'gemini-1.5-flash',
                    'total_tokens': self._estimate_tokens(prompt, response.text),
                    'session_turns': len(self.chat_session.history)
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating insights from results: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def generate_chart_specifications(self, chart_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate Vega-Lite chart specifications from chart context
        
        Args:
            chart_context: Context containing aggregated data and metadata
            
        Returns:
            Dict containing Vega-Lite chart specifications
        """
        try:
            # Build the chart generation prompt
            prompt = self._build_chart_prompt(chart_context)
            
            # Generate chart specs using current session or new one
            logger.info("📊 Generating Vega-Lite chart specifications using Gemini API")
            
            if self.chat_session:
                # Use existing session to maintain context
                response = self.chat_session.send_message(prompt)
            else:
                # Create new session for chart generation
                response = self.model.generate_content(prompt, generation_config=self.generation_config)
            
            # Parse and structure the response
            chart_specs = self._parse_response(response.text)
            
            # Log the AI-generated chart specs
            logger.info("📈 AI CHART SPECIFICATIONS GENERATED:")
            logger.info("=" * 50)
            logger.info(json.dumps(chart_specs, indent=2, default=str))
            logger.info("=" * 50)
            
            logger.info("Successfully generated Vega-Lite chart specifications")
            return {
                'success': True,
                'chart_specs': chart_specs,
                'metadata': {
                    'model': 'gemini-1.5-flash',
                    'total_tokens': self._estimate_tokens(prompt, response.text),
                    'chart_count': len(chart_specs) if isinstance(chart_specs, list) else 1
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating chart specifications: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def generate_aggregation_plan(self, metadata: Dict[str, Any], user_query: str = "Generate summary insights") -> Dict[str, Any]:
        """
        Legacy method - generates aggregation plan without session
        For backward compatibility
        """
        return self.start_analysis_session(metadata, user_query)
    
    def _build_aggregation_prompt(self, metadata: Dict[str, Any], user_query: str) -> str:
        """Build Pandas aggregation prompt with metadata and user query"""
        
        return f"""You are an expert Data Analyst and Pandas specialist for restaurant analytics.

### CONTEXT:
Generate a comprehensive **Pandas aggregation plan** in JSON format for restaurant data analysis.
This plan will be executed by an automated system to create business insights and visualizations.

### AVAILABLE PANDAS OPERATIONS:

#### AGGREGATION FUNCTIONS:
- **Statistical**: `mean`, `median`, `sum`, `min`, `max`, `std`, `var`, `quantile`, `skew`, `kurt`
- **Counting**: `count`, `nunique`, `size`
- **Selection**: `first`, `last`, `nth`, `mode`
- **Cumulative**: `cumsum`, `cummax`, `cummin`, `cumprod`
- **Custom**: `agg(lambda x: custom_function)`

#### GROUPING & FILTERING:
- **GroupBy**: Group by single/multiple columns (location, date, category, etc.)
- **Filtering**: Filter by conditions (`>`, `<`, `==`, `isin`, `between`)
- **Time Operations**: `resample`, `rolling`, date-based grouping
- **Sorting**: `sort_values`, `nlargest`, `nsmallest`

#### RESTAURANT-SPECIFIC AGGREGATIONS:
- **Sales Analysis**: Daily/weekly/monthly totals, averages, growth rates
- **Location Performance**: Compare locations, identify top/bottom performers  
- **Category Analysis**: Food vs drinks, menu item performance
- **Time Analysis**: Peak hours, seasonal trends, day-of-week patterns
- **Efficiency Metrics**: Sales per hour, average transaction size
- **Ranking**: Top selling items, best locations, peak times


### STRICT REQUIREMENTS:
1. **ONLY use tables and columns from DATA SCHEMA below** - DO NOT invent columns
2. **Generate exactly 3-4 operations maximum** for focused analysis
3. **Each operation must have clear business value** - sales, efficiency, or performance
4. **Use ONLY these aggregation functions**: `sum`, `mean`, `count`, `min`, `max`, `std`, `median`
5. **Include sorting and reasonable limits** (5-20 rows max per operation)
6. **Output names must be descriptive** and explain the business purpose

### MANDATORY JSON SCHEMA:
```json
{{
  "operations": [
    {{
      "name": "clear_business_purpose_name",
      "type": "groupby",
      "table": "exact_table_name_from_schema",
      "groupby": ["exact_column_name"],
      "agg_functions": {{
        "exact_column_name": ["sum", "mean", "count"]
      }},
      "sort": [{{
        "column": "exact_result_column_name", 
        "ascending": false
      }}],
      "limit": 10,
      "output_name": "descriptive_result_name"
    }}
  ]
}}
```

### BUSINESS-FIRST OPERATION TYPES:
- **Sales Performance**: Total sales, average transaction size, sales trends
- **Location Analysis**: Compare store/location performance 
- **Category Performance**: Compare product categories (food vs drinks)
- **Time Analysis**: Daily/weekly patterns (if date columns available)

### CRITICAL CONSTRAINTS:
- ❌ DO NOT reference columns not in the schema
- ❌ DO NOT use complex aggregations (quantile, skew, etc.)
- ❌ DO NOT generate more than 4 operations
- ❌ DO NOT use joins unless explicitly needed
- ✅ DO focus on immediate business decisions
- ✅ DO use exact column names from schema
- ✅ DO include meaningful limits and sorting

---

### AVAILABLE DATA SCHEMA:
{json.dumps(metadata, indent=2, default=str)}

### BUSINESS REQUEST:
{user_query}

### OUTPUT REQUIREMENT:
Return ONLY a valid JSON object matching the schema above. No explanations, no additional text."""
    
    def _build_insights_prompt(self, execution_results: Dict[str, Any]) -> str:
        """Build prompt for generating insights from execution results"""
        
        return f"""You are a Business Intelligence Analyst creating a concise restaurant performance report.

### STRICT MARKDOWN REQUIREMENTS:

#### MANDATORY REPORT STRUCTURE:
```markdown
# Restaurant Performance Analysis

**Report Date:** {{current_date}}

## 🎯 Executive Summary
{{2-3 sentences max - key performance and immediate action needed}}

## 📊 Top 3 Key Findings
1. **{{Metric/Area}}**: {{Specific finding with number}} → **{{Impact}}**
2. **{{Metric/Area}}**: {{Specific finding with number}} → **{{Impact}}**
3. **{{Metric/Area}}**: {{Specific finding with number}} → **{{Impact}}**

## ⚡ Immediate Actions Required
1. {{Specific action}} - Expected impact: {{outcome}}
2. {{Specific action}} - Expected impact: {{outcome}}
3. {{Specific action}} - Expected impact: {{outcome}}

## 📈 Performance Highlights
- **Best Performer:** {{specific item/location}} ({{metric}})
- **Needs Attention:** {{specific item/location}} ({{metric}})
- **Growth Opportunity:** {{specific recommendation}}
```

### CRITICAL CONSTRAINTS:
- ❌ NO lengthy paragraphs - use bullet points and short sentences
- ❌ NO more than 3 key findings maximum
- ❌ NO generic insights - be specific with numbers and actions
- ❌ NO technical jargon - restaurant manager language only
- ✅ DO include specific metrics and percentages from data
- ✅ DO focus on immediate business decisions
- ✅ DO use emojis for visual clarity
- ✅ DO make every insight actionable

### BUSINESS FOCUS AREAS:
- **Revenue**: Sales totals, averages, growth/decline patterns
- **Performance**: Best/worst performing locations, categories, periods
- **Efficiency**: Cost ratios, sales per unit metrics
- **Trends**: Notable increases, decreases, or patterns

### TONE REQUIREMENTS:
- Direct and decisive
- Numbers-driven
- Action-oriented
- Restaurant operations focused

### DATA TO ANALYZE:
{json.dumps(execution_results, indent=2, default=str)}

### OUTPUT REQUIREMENT:
Return ONLY a concise markdown report following the exact structure above. Focus on the top 3 most important business decisions that need immediate attention."""
    
    def _build_chart_prompt(self, chart_context: Dict[str, Any]) -> str:
        """Build prompt for generating Vega-Lite chart specifications"""
        
        return f"""You are a Data Visualization Expert creating restaurant business dashboards.

### STRICT VEGA-LITE REQUIREMENTS:

#### MANDATORY JSON SCHEMA:
```json
[
  {{
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "title": "Clear Business Purpose Title",
    "width": 400,
    "height": 250,
    "data": {{"url": "CSV_DATA_SOURCE_URL"}},
    "mark": {{"type": "bar|line|point|area", "tooltip": true}},
    "encoding": {{
      "x": {{"field": "exact_column_name", "type": "nominal|quantitative|temporal", "title": "Clear Axis Label"}},
      "y": {{"field": "exact_column_name", "type": "quantitative", "title": "Clear Axis Label"}},
      "tooltip": [
        {{"field": "column1", "type": "nominal", "title": "Display Name"}},
        {{"field": "column2", "type": "quantitative", "format": ".2f", "title": "Value"}}
      ]
    }}
  }}
]
```

#### CHART TYPE RULES:
- **Bar Charts**: For comparing categories (locations, categories, top items)
- **Line Charts**: For time series trends (daily/weekly sales)
- **Point Charts**: For scatter plots and correlation analysis
- **Area Charts**: For cumulative or stacked data over time

#### BUSINESS CHART PRIORITIES:
1. **Sales Comparison Chart** - Compare performance across locations/categories
2. **Trend Analysis Chart** - Show sales patterns over time (if dates available)
3. **Top Performers Chart** - Highlight best/worst performing items or locations

### CRITICAL CONSTRAINTS:
- ❌ DO NOT embed data in charts - use external CSV URLs only
- ❌ DO NOT reference columns not in the aggregation results  
- ❌ DO NOT generate more than 3 charts maximum
- ❌ DO NOT use complex chart types (heatmaps, geographic, etc.)
- ❌ DO NOT invent or hallucinate column names (like "Total", "Server", "Tip")
- ✅ DO use "CSV_DATA_SOURCE_URL" placeholder for data source
- ✅ DO use ONLY exact column names from aggregation results below
- ✅ DO include meaningful tooltips for business users
- ✅ DO use professional titles that explain business value
- ✅ DO set fixed width/height for consistency

### COLUMN NAME VALIDATION:
**CRITICAL: ONLY use these exact column names found in the aggregation results:**

### AVAILABLE AGGREGATION RESULTS:
{json.dumps(chart_context, indent=2, default=str)}

### DATA SOURCE INSTRUCTIONS:
- Use "CSV_DATA_SOURCE_URL" as placeholder for data.url
- The system will replace this with actual S3 CSV URLs after chart generation
- Focus on chart structure and column mappings only
- DO NOT include any actual data in the specifications

### OUTPUT REQUIREMENT:
Return a JSON array of exactly 2-3 Vega-Lite chart specifications using external CSV data sources. Each chart must:
1. Reference "CSV_DATA_SOURCE_URL" for data loading
2. Use ONLY the exact column names shown in the aggregation results above
3. NOT invent any column names not explicitly listed
4. Match field names exactly (case-sensitive)

**EXAMPLE**: If aggregation shows columns ["Location", "Sales", "Orders"], only use these exact names."""
    

    
    def _parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse and structure the AI response"""
        try:
            # Try to extract JSON from the response
            # Handle cases where response might have markdown code blocks
            cleaned_text = response_text.strip()
            if cleaned_text.startswith('```json'):
                cleaned_text = cleaned_text[7:]  # Remove ```json
            if cleaned_text.endswith('```'):
                cleaned_text = cleaned_text[:-3]  # Remove trailing ```
            
            # Try to parse as JSON first
            try:
                return json.loads(cleaned_text)
            except json.JSONDecodeError:
                # If JSON parsing fails, structure as text response
                return {
                    'summary': response_text[:500] + "..." if len(response_text) > 500 else response_text,
                    'full_response': response_text,
                    'insights': {
                        'key_findings': ['Analysis completed - see full_response for details'],
                        'recommendations': ['Review full analysis in full_response field'],
                        'data_quality': 'Metadata processed successfully'
                    },
                    'format': 'text'
                }
        except Exception as e:
            logger.warning(f"Error parsing response: {e}")
            return {
                'summary': 'Analysis completed but response parsing encountered issues',
                'full_response': response_text,
                'parsing_error': str(e),
                'format': 'text'
            }
    
    def _estimate_tokens(self, prompt: str, response: str) -> int:
        """Rough token estimation (4 chars ≈ 1 token)"""
        return (len(prompt) + len(response)) // 4

    def test_connection(self) -> Dict[str, Any]:
        """Test connection to Gemini API"""
        try:
            test_prompt = "Hello, this is a connection test. Please respond with 'Connection successful'."
            response = self.model.generate_content(test_prompt)
            return {
                'success': True,
                'response': response.text,
                'model': 'gemini-1.5-flash'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
