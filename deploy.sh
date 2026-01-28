#!/bin/bash

# Decision-Ready Restaurant Reports - Simple Deployment Script
# Deploys to Toast AWS environment with hardcoded working values

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Hardcoded values that we know work
STACK_NAME="restaurant-reports-dev"
REGION="us-east-1"
ENVIRONMENT="dev"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}[DEPLOY]${NC} $1"
}

print_header "🚀 Decision-Ready Restaurant Reports Deployment"
print_status "Stack: $STACK_NAME"
print_status "Region: $REGION"
print_status "Using existing IAM role: lambda_s3"

# Prerequisite checks
print_status "Checking prerequisites..."

if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

if ! command -v sam &> /dev/null; then
    print_error "SAM CLI is not installed. Please install it first."
    exit 1
fi

if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured or expired."
    print_error "Please ensure Toast auth is working."
    exit 1
fi

# Check requirements file exists
if [ ! -f "requirements.txt" ]; then
    print_error "requirements.txt not found."
    exit 1
fi

# Python compilation check
print_status "Checking Python syntax..."
if ! python3 -m py_compile decision_ready_report.py; then
    print_error "Python compilation failed! Fix syntax errors before deploying."
    exit 1
fi

# Check imports for ZIP deployments only (container deps are installed during build)
if [ ! -f "Dockerfile" ]; then
    print_status "Checking imports for ZIP deployment..."
    if ! python3 -c "
import sys
sys.path.append('.')
try:
    from decision_ready_report import lambda_handler
    print('✅ Import check passed')
except ImportError as e:
    print(f'❌ Import error: {e}')
    sys.exit(1)
except Exception as e:
    print(f'❌ Other error: {e}')
    sys.exit(1)
"; then
        print_error "Import validation failed! Fix import errors before deploying."
        exit 1
    fi
else
    print_status "Container deployment - skipping local import validation"
    print_status "(Dependencies will be installed in container during build)"
fi

# Build the SAM application
print_status "Building SAM application..."
if [ -f "Dockerfile" ]; then
    print_status "Container build detected - using --use-container"
    if ! sam build --use-container; then
        print_error "SAM build failed!"
        exit 1
    fi
else
    if ! sam build; then
        print_error "SAM build failed!"
        exit 1
    fi
fi

# Check if this is a container build (containers don't have size limits)
if [ -f "Dockerfile" ]; then
    print_status "Container Lambda build - no size limits"
else
    # Check package size for ZIP deployments only
    PACKAGE_SIZE=$(du -sm .aws-sam/build/RestaurantReportsFunction/ 2>/dev/null | cut -f1)
    if [ ! -z "$PACKAGE_SIZE" ] && [ "$PACKAGE_SIZE" -gt 200 ]; then
        print_warning "Package size is large (${PACKAGE_SIZE}MB). Deployment might be slow."
    else
        print_status "Package size: ${PACKAGE_SIZE}MB"
    fi
fi

# Deploy the application
print_status "Deploying to AWS Lambda..."

# Check if this is a container deployment
if [ -f "Dockerfile" ]; then
    print_status "Container deployment detected"
    if ! sam deploy \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --parameter-overrides Environment="$ENVIRONMENT" \
        --capabilities CAPABILITY_IAM \
        --resolve-s3 \
        --resolve-image-repos \
        --no-confirm-changeset \
        --no-fail-on-empty-changeset; then
        print_error "Deployment failed!"
        exit 1
    fi
else
    print_status "ZIP deployment detected"
    if ! sam deploy \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --parameter-overrides Environment="$ENVIRONMENT" \
        --capabilities CAPABILITY_IAM \
        --resolve-s3 \
        --no-confirm-changeset \
        --no-fail-on-empty-changeset; then
        print_error "Deployment failed!"
        exit 1
    fi
fi

# Get the API endpoint
print_status "Retrieving API endpoint..."
API_ENDPOINT=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`RestaurantReportsApi`].OutputValue' \
    --output text 2>/dev/null)

if [ -n "$API_ENDPOINT" ] && [ "$API_ENDPOINT" != "None" ]; then
    print_header "✅ Deployment Successful!"
    echo
    print_status "🌐 API Endpoint: $API_ENDPOINT"
    print_status "📦 Lambda Function: $STACK_NAME-RestaurantReportsFunction"
    print_status "🏗️  Stack: $STACK_NAME"
    print_status "🌍 Region: $REGION"
    echo
    
    # Test the deployment
    print_status "🧪 Testing deployment..."
    TEST_RESPONSE=$(curl -s -X POST "$API_ENDPOINT" \
        -H "Content-Type: application/json" \
        -d '{
            "csv_data": "Date,Sales\n2024-01-01,1000",
            "file_name": "deployment_test"
        }' | head -c 100)
    
    if echo "$TEST_RESPONSE" | grep -q "CSV processed successfully"; then
        print_status "✅ API test successful!"
    else
        print_warning "⚠️  API test failed or returned unexpected response"
    fi
    
    echo
    print_header "🎯 Usage Example:"
    echo "curl -X POST $API_ENDPOINT \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{\"csv_data\": \"Date,Sales\\n2024-01-01,1500\", \"file_name\": \"my_data\"}'"
    echo
else
    print_warning "Deployment completed but could not retrieve API endpoint."
    print_status "Check AWS Console for details."
fi
