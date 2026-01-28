#!/usr/bin/env python3
"""
Test utility for Decision-Ready Restaurant Reports
Tests ZIP file processing via API Gateway deployment
"""

import argparse
import base64
import json
import sys
import requests
from pathlib import Path

# API Gateway endpoint (update this to your deployed endpoint)
API_ENDPOINT = "https://adsbyrhf4l.execute-api.us-east-1.amazonaws.com/Stage/process/"

def upload_zip_file(zip_path):
    """
    Upload and process a ZIP file via the deployed API
    
    Args:
        zip_path (str): Path to the ZIP file to process
    """
    zip_file = Path(zip_path)
    
    if not zip_file.exists():
        print(f"❌ Error: ZIP file not found: {zip_path}")
        return False
    
    if not zip_file.suffix.lower() == '.zip':
        print(f"❌ Error: File must be a ZIP file: {zip_path}")
        return False
    
    print(f"📁 Reading ZIP file: {zip_file.name}")
    print(f"📊 File size: {zip_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Read and encode ZIP file
    try:
        with open(zip_file, 'rb') as f:
            zip_data = f.read()
        
        zip_base64 = base64.b64encode(zip_data).decode('utf-8')
        print(f"✅ ZIP file encoded successfully")
        
    except Exception as e:
        print(f"❌ Error reading ZIP file: {e}")
        return False
    
    # Prepare request payload
    payload = {
        "zip_base64": zip_base64,
        "file_name": zip_file.stem
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    print(f"🚀 Sending request to API Gateway...")
    print(f"🌐 Endpoint: {API_ENDPOINT}")
    print("⏱️  Note: First request after idle may take ~20s (cold start)")
    
    try:
        # Send request with timeout (allow for cold start)
        response = requests.post(
            API_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=600  # 10 minutes timeout to handle cold starts + processing
        )
        
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Success! Processing completed.")
            
            try:
                response_data = response.json()
                
                # Check new S3-based response format
                delivery_method = response_data.get('delivery_method', 'unknown')
                print(f"📦 Delivery Method: {delivery_method}")
                
                if delivery_method == 'S3_dashboard':
                    print("🎉 NEW S3 DASHBOARD FORMAT!")
                    print("=" * 50)
                    
                    # Show main response info
                    print(f"📄 Report Name: {response_data.get('file_name', 'N/A')}")
                    print(f"🆔 Request ID: {response_data.get('request_id', 'N/A')}")
                    
                    # Show S3 reports
                    if 'reports' in response_data:
                        reports = response_data['reports']
                        print("\n📊 GENERATED REPORTS:")
                        
                        if 'business_analysis' in reports:
                            print(f"  📝 Business Analysis (Markdown):")
                            print(f"     {reports['business_analysis']}")
                        
                        if 'interactive_dashboard' in reports:
                            print(f"  🌐 Interactive Dashboard (HTML):")
                            print(f"     {reports['interactive_dashboard']}")
                        
                        if 'data_sources' in reports and isinstance(reports['data_sources'], dict):
                            print(f"  📁 Data Sources ({len(reports['data_sources'])} CSV files):")
                            for source_name, source_url in reports['data_sources'].items():
                                print(f"     • {source_name}: {source_url}")
                    
                    # Show AI summary
                    if 'ai_summary' in response_data:
                        ai_summary = response_data['ai_summary']
                        print(f"\n🤖 AI Summary Preview:")
                        print(f"   {ai_summary[:200]}...")
                    
                    # Show instructions
                    if 'instructions' in response_data:
                        instructions = response_data['instructions']
                        print(f"\n📋 Usage Instructions:")
                        for key, instruction in instructions.items():
                            print(f"   • {key}: {instruction}")
                
                elif delivery_method == 'basic_insights':
                    print("⚠️  S3 upload failed - basic insights returned")
                    if 'ai_insights' in response_data:
                        print("🧠 AI Insights available (limited format)")
                
                else:
                    # Legacy format fallback
                    print(f"📄 Generated file: {response_data.get('file_name', 'N/A')}")
                    
                    # Check if we got insights (legacy)
                    if 'insights' in response_data:
                        insights = response_data['insights']
                        if insights and isinstance(insights, dict):
                            print(f"🧠 AI Insights available: {len(insights)} insights")
                            
                            # Show AI summary if available
                            if 'ai_summary' in insights:
                                print("🤖 AI Summary:")
                                print(f"   {insights['ai_summary'][:200]}...")
                        else:
                            print("🧠 AI Insights: Processing completed (no AI summary generated)")
                    
                    # Check if we got metadata (legacy)
                    if 'metadata' in response_data:
                        metadata = response_data['metadata']
                        if metadata and isinstance(metadata, dict):
                            print(f"📋 Metadata: {len(metadata)} CSV files processed")
                
                # ALWAYS PRINT FULL RESPONSE FOR DEBUGGING
                print("\n" + "="*60)
                print("🔍 FULL LAMBDA RESPONSE (for debugging):")
                print("="*60)
                print(json.dumps(response_data, indent=2, default=str))
                print("="*60)
                
                # Check for download URL (large files)
                if 'download_url' in response_data:
                    print(f"🔗 Download URL: {response_data['download_url']}")
                    print("   (File was too large for direct response)")
                
                # Save Excel file if included in response
                if 'excel_file' in response_data:
                    excel_data = base64.b64decode(response_data['excel_file'])
                    output_file = f"{zip_file.stem}_processed.xlsx"
                    
                    with open(output_file, 'wb') as f:
                        f.write(excel_data)
                    
                    print(f"💾 Excel file saved: {output_file}")
                    print(f"📊 Size: {len(excel_data)} bytes")
                
                return True
                
            except json.JSONDecodeError:
                print("⚠️  Response is not valid JSON")
                print(f"Raw response: {response.text[:500]}")
                return False
        
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("⏰ Request timed out (file might be too large or processing is slow)")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
        return False

def test_csv_data():
    """
    Test with enhanced CSV data and new S3 response format
    """
    print("🧪 Testing with enhanced restaurant CSV data...")
    print("⏱️  Note: First request after idle may take ~20s (cold start)")
    
    payload = {
        "csv_data": "Date,Sales,Location,Category,Orders\n2024-01-01,1500,NYC,Food,50\n2024-01-02,1800,LA,Drinks,60\n2024-01-03,1200,Chicago,Food,40\n2024-01-04,2100,NYC,Food,70\n2024-01-05,1900,Miami,Drinks,65",
        "file_name": "restaurant_test_data"
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(
            API_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=120  # 2 minutes to handle cold start
        )
        
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ CSV test successful!")
            response_data = response.json()
            
            # Check new S3-based response format
            delivery_method = response_data.get('delivery_method', 'unknown')
            print(f"📦 Delivery Method: {delivery_method}")
            
            if delivery_method == 'S3_dashboard':
                print("🎉 NEW S3 DASHBOARD FORMAT!")
                print("=" * 40)
                
                print(f"📄 Report: {response_data.get('file_name', 'N/A')}")
                print(f"🆔 Request ID: {response_data.get('request_id', 'N/A')}")
                
                # Show S3 reports
                if 'reports' in response_data:
                    reports = response_data['reports']
                    print("\n📊 AVAILABLE REPORTS:")
                    
                    if 'business_analysis' in reports:
                        print(f"  📝 Markdown Report:")
                        print(f"     {reports['business_analysis']}")
                    
                    if 'interactive_dashboard' in reports:
                        print(f"  🌐 HTML Dashboard:")
                        print(f"     {reports['interactive_dashboard']}")
                    
                    if 'data_sources' in reports:
                        print(f"  📁 CSV Data Sources:")
                        if isinstance(reports['data_sources'], dict):
                            for name, url in reports['data_sources'].items():
                                print(f"     • {name}: {url}")
                        else:
                            print(f"     {reports['data_sources']}")
                
                print(f"\n🤖 AI Summary: {response_data.get('ai_summary', 'No summary')[:100]}...")
                
            else:
                # Fallback for legacy format
                print(f"📄 Generated file: {response_data.get('file_name', 'N/A')}")
            
            # ALWAYS PRINT FULL RESPONSE FOR DEBUGGING
            print("\n" + "="*60)
            print("🔍 FULL LAMBDA RESPONSE (for debugging):")
            print("="*60)
            print(json.dumps(response_data, indent=2, default=str))
            print("="*60)
                
            return True
        else:
            print(f"❌ CSV test failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ CSV test error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Test Decision-Ready Restaurant Reports API')
    parser.add_argument('--upload-zip', type=str, help='Path to ZIP file to upload and process')
    parser.add_argument('--test-csv', action='store_true', help='Test with simple CSV data')
    parser.add_argument('--endpoint', type=str, help='Override API endpoint URL')
    
    args = parser.parse_args()
    
    # Override endpoint if provided
    if args.endpoint:
        global API_ENDPOINT
        API_ENDPOINT = args.endpoint
        print(f"🌐 Using custom endpoint: {API_ENDPOINT}")
    
    print("🍕 Decision-Ready Restaurant Reports - Test Utility")
    print("=" * 50)
    
    success = False
    
    if args.upload_zip:
        success = upload_zip_file(args.upload_zip)
    elif args.test_csv:
        success = test_csv_data()
    else:
        print("❌ Please specify either --upload-zip or --test-csv")
        parser.print_help()
        return 1
    
    if success:
        print("\n🎉 Test completed successfully!")
        return 0
    else:
        print("\n💥 Test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
