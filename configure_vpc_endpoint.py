import os
import sys
import boto3
from botocore.exceptions import ClientError


def get_vpc_id():
    """Get the VPC ID of the current instance."""
    try:
        # Get instance metadata
        response = requests.get(
            "http://169.254.169.254/latest/meta-data/vpc-id", timeout=1
        )
        return response.text.strip()
    except:
        # If we're not in an EC2 instance, use the VPC ID from environment
        return os.environ.get("VPC_ID")


def configure_vpc_endpoint():
    """Configure VPC Endpoint for RDS access."""
    try:
        # Create EC2 client
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
        )

        vpc_id = get_vpc_id()
        if not vpc_id:
            print("Could not determine VPC ID")
            return False

        print(f"Using VPC: {vpc_id}")

        # Get subnet IDs in the VPC
        subnets = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["Subnets"]

        if not subnets:
            print("No subnets found in VPC")
            return False

        # Use the first subnet for the endpoint
        subnet_id = subnets[0]["SubnetId"]
        print(f"Using subnet: {subnet_id}")

        # Check if endpoint already exists
        existing_endpoints = ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {
                    "Name": "service-name",
                    "Values": [f"com.amazonaws.{os.environ['AWS_REGION']}.rds"],
                },
            ]
        )["VpcEndpoints"]

        if existing_endpoints:
            print("VPC Endpoint already exists")
            return True

        # Create VPC Endpoint
        try:
            response = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName=f"com.amazonaws.{os.environ['AWS_REGION']}.rds",
                SubnetIds=[subnet_id],
                VpcEndpointType="Interface",
                TagSpecifications=[
                    {
                        "ResourceType": "vpc-endpoint",
                        "Tags": [
                            {"Key": "Name", "Value": "github-actions-rds-endpoint"},
                            {"Key": "Environment", "Value": "github-actions"},
                        ],
                    }
                ],
            )
            print(f"Created VPC Endpoint: {response['VpcEndpoint']['VpcEndpointId']}")
            return True
        except ClientError as e:
            print(f"Error creating VPC Endpoint: {e}")
            return False

    except Exception as e:
        print(f"Error configuring VPC Endpoint: {e}")
        return False


if __name__ == "__main__":
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "VPC_ID",  # Optional if running in EC2
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        print("Missing required environment variables:", ", ".join(missing))
        sys.exit(1)

    if not configure_vpc_endpoint():
        sys.exit(1)
