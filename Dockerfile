# Use AWS Lambda Python base image (this gives us the Lambda runtime)
FROM public.ecr.aws/lambda/python:3.9

# Copy requirements file first (for better Docker caching)
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install -r requirements.txt --target ${LAMBDA_TASK_ROOT}

# Copy all our source code
COPY . ${LAMBDA_TASK_ROOT}

# Tell Lambda which function to run (our main handler)
CMD ["decision_ready_report.lambda_handler"]
