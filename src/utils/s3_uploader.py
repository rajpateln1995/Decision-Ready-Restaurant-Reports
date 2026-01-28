"""
S3 uploader for restaurant reports outputs
"""
import boto3
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class S3ReportUploader:
    """Handles uploading restaurant reports to S3 with organized structure"""
    
    def __init__(self, bucket_name: str = "restaurant-reports-outputs"):
        """
        Initialize S3 uploader
        
        Args:
            bucket_name: S3 bucket name for storing reports
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ S3 bucket '{self.bucket_name}' exists")
        except:
            try:
                # Create bucket
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"✅ Created S3 bucket '{self.bucket_name}'")
                
                # Set bucket policy for public read access to reports
                self._set_bucket_policy()
            except Exception as e:
                logger.error(f"❌ Failed to create S3 bucket: {e}")
                raise
    
    def _set_bucket_policy(self):
        """Set bucket policy for public read access to reports"""
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*"
                }
            ]
        }
        
        try:
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=json.dumps(policy)
            )
            logger.info("✅ Set bucket policy for public read access")
        except Exception as e:
            logger.warning(f"⚠️ Could not set bucket policy: {e}")
    
    def upload_report_files(self, 
                           markdown_content: str,
                           html_dashboard: str,
                           csv_sources: Dict[str, Any],
                           request_id: Optional[str] = None) -> Dict[str, str]:
        """
        Upload report files to S3 with organized folder structure
        
        Args:
            markdown_content: AI-generated markdown report
            html_dashboard: HTML dashboard with charts
            csv_sources: CSV data sources for charts
            request_id: Unique request identifier
            
        Returns:
            Dict containing S3 URLs for uploaded files
        """
        if not request_id:
            request_id = self._generate_request_id()
        
        logger.info(f"📤 STARTING S3 UPLOAD")
        logger.info(f"🆔 Request ID: {request_id}")
        logger.info(f"📁 S3 Bucket: {self.bucket_name}")
        
        uploaded_files = {}
        
        try:
            # Create folder structure: reports/{date}/{request_id}/
            date_folder = datetime.now().strftime("%Y-%m-%d")
            base_path = f"reports/{date_folder}/{request_id}"
            
            logger.info(f"📂 Upload Path: {base_path}")
            
            # 1. Upload markdown report
            markdown_key = f"{base_path}/business-report.md"
            markdown_url = self._upload_content(
                content=markdown_content,
                key=markdown_key,
                content_type="text/markdown; charset=utf-8"
            )
            uploaded_files['markdown_report'] = markdown_url
            logger.info(f"📝 Uploaded markdown: {markdown_key}")
            
            # 2. Upload HTML dashboard
            html_key = f"{base_path}/dashboard.html"
            html_url = self._upload_content(
                content=html_dashboard,
                key=html_key,
                content_type="text/html; charset=utf-8"
            )
            uploaded_files['html_dashboard'] = html_url
            logger.info(f"🌐 Uploaded dashboard: {html_key}")
            
            # 3. Upload CSV data sources
            csv_urls = {}
            for source_name, source_data in csv_sources.items():
                csv_content = self._convert_to_csv(source_data['data'])
                csv_key = f"{base_path}/data/{source_data['filename']}"
                csv_url = self._upload_content(
                    content=csv_content,
                    key=csv_key,
                    content_type="text/csv"
                )
                csv_urls[source_name] = csv_url
                logger.info(f"📊 Uploaded CSV: {csv_key}")
            
            uploaded_files['csv_sources'] = csv_urls
            
            # 4. Create and upload metadata
            metadata = {
                'request_id': request_id,
                'upload_timestamp': datetime.now().isoformat(),
                'files': {
                    'markdown_report': markdown_key,
                    'html_dashboard': html_key,
                    'csv_sources': list(csv_urls.keys())
                },
                'public_urls': uploaded_files
            }
            
            metadata_key = f"{base_path}/metadata.json"
            metadata_url = self._upload_content(
                content=json.dumps(metadata, indent=2),
                key=metadata_key,
                content_type="application/json"
            )
            uploaded_files['metadata'] = metadata_url
            logger.info(f"📋 Uploaded metadata: {metadata_key}")
            
            logger.info("✅ S3 UPLOAD COMPLETED SUCCESSFULLY")
            logger.info(f"📤 Files uploaded: {len(uploaded_files)}")
            
            return {
                'success': True,
                'request_id': request_id,
                'bucket': self.bucket_name,
                'base_path': base_path,
                'urls': uploaded_files
            }
            
        except Exception as e:
            logger.error(f"❌ S3 upload failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'request_id': request_id,
                'urls': uploaded_files  # Partial uploads
            }
    
    def _upload_content(self, content: str, key: str, content_type: str) -> str:
        """Upload content to S3 and return public URL"""
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=content.encode('utf-8'),
            ContentType=content_type,
            CacheControl="max-age=3600"  # 1 hour cache
        )
        
        # Return public URL
        return f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
    
    def _convert_to_csv(self, data: list) -> str:
        """Convert list of dicts to CSV format"""
        if not data or not isinstance(data[0], dict):
            return ""
        
        import csv
        from io import StringIO
        
        output = StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID"""
        timestamp = datetime.now().strftime("%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"req_{timestamp}_{unique_id}"
    
    def get_bucket_info(self) -> Dict[str, Any]:
        """Get bucket information and statistics"""
        try:
            # Get bucket location
            location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)
            
            # List recent objects
            objects = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix="reports/",
                MaxKeys=10
            )
            
            return {
                'bucket_name': self.bucket_name,
                'region': location.get('LocationConstraint', 'us-east-1'),
                'recent_reports': len(objects.get('Contents', [])),
                'bucket_url': f"https://{self.bucket_name}.s3.amazonaws.com"
            }
        except Exception as e:
            logger.error(f"Error getting bucket info: {e}")
            return {'error': str(e)}
