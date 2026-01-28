#!/usr/bin/env python3
"""
Simple deployment test for the Lambda function
"""
import json
import base64

# Sample CSV data for testing
SAMPLE_CSV = """date,item_name,category,quantity_sold,unit_price,total_revenue,labor_cost
2024-01-01,Burger Deluxe,Entrees,45,12.99,584.55,150.00
2024-01-01,Caesar Salad,Salads,23,8.99,206.77,45.00
2024-01-01,Craft Beer,Beverages,67,5.99,401.33,25.00
2024-01-02,Burger Deluxe,Entrees,52,12.99,675.48,160.00
2024-01-02,Caesar Salad,Salads,18,8.99,161.82,35.00"""

def create_csv_test_event():
    """Create test event with CSV data"""
    return {
        "body": json.dumps({
            "csv_data": SAMPLE_CSV,
            "file_name": "sample_sales.csv"
        })
    }

def create_zip_test_event():
    """Create test event with ZIP data (base64 encoded)"""
    # Create a simple ZIP file with the CSV
    import zipfile
    from io import BytesIO
    
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('sales_data.csv', SAMPLE_CSV)
    
    zip_bytes = zip_buffer.getvalue()
    zip_base64 = base64.b64encode(zip_bytes).decode('utf-8')
    
    return {
        "body": json.dumps({
            "zip_base64": zip_base64,
            "file_name": "sales_data.zip"
        })
    }

def test_lambda_locally():
    """Test the lambda function locally"""
    print("🧪 Testing Lambda function locally...")
    
    # Import the lambda handler
    try:
        from decision_ready_report import lambda_handler
    except Exception as e:
        print(f"❌ Failed to import lambda handler: {e}")
        return False
    
    # Test CSV processing
    print("\n📄 Testing CSV processing...")
    csv_event = create_csv_test_event()
    
    try:
        response = lambda_handler(csv_event, {})
        print(f"✅ CSV test successful! Status: {response.get('statusCode', 'Unknown')}")
        
        # Parse response body
        if response.get('body'):
            body = json.loads(response['body'])
            if body.get('success'):
                print(f"   Reports processed: {body.get('reports_processed', 0)}")
                if 'ai_insights' in body:
                    print("   🤖 AI insights included!")
                else:
                    print("   ⚠️  No AI insights (check API key)")
            else:
                print(f"   ❌ Processing failed: {body.get('error', 'Unknown error')}")
        
    except Exception as e:
        print(f"❌ CSV test failed: {e}")
        return False
    
    # Test ZIP processing
    print("\n📦 Testing ZIP processing...")
    zip_event = create_zip_test_event()
    
    try:
        response = lambda_handler(zip_event, {})
        print(f"✅ ZIP test successful! Status: {response.get('statusCode', 'Unknown')}")
        
        # Parse response body
        if response.get('body'):
            body = json.loads(response['body'])
            if body.get('success'):
                print(f"   Reports processed: {body.get('reports_processed', 0)}")
                if 'ai_insights' in body:
                    print("   🤖 AI insights included!")
                else:
                    print("   ⚠️  No AI insights (check API key)")
            else:
                print(f"   ❌ Processing failed: {body.get('error', 'Unknown error')}")
                
    except Exception as e:
        print(f"❌ ZIP test failed: {e}")
        return False
    
    return True

def main():
    """Run deployment tests"""
    print("🚀 Lambda Deployment Test")
    print("=" * 40)
    
    # Check if API key is configured
    print("⚠️  Remember to set your GEMINI_API_KEY environment variable")
    print("   export GEMINI_API_KEY='your_api_key_here'")
    print()
    
    success = test_lambda_locally()
    
    print("\n" + "=" * 40)
    if success:
        print("🎉 All tests passed! Ready to deploy with ./deploy.sh")
    else:
        print("❌ Some tests failed - check the errors above")
    
    return success

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
