import os
import sys
import boto3
from botocore.exceptions import ClientError


def cleanup_vpc_endpoint():
    """Clean up the VPC Endpoint created for GitHub Actions."""
    try:
        ec2 = boto3.client("ec2")
        vpc_id = os.getenv("VPC_ID")

        if not vpc_id:
            print("Error: VPC_ID environment variable not set")
            sys.exit(1)

        print(f"Looking for VPC Endpoint in VPC {vpc_id}...")

        # Describe VPC Endpoints
        response = ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": ["com.amazonaws.us-east-1.rds"]},
            ]
        )

        endpoints = response.get("VpcEndpoints", [])

        if not endpoints:
            print("No VPC Endpoints found to clean up")
            return

        for endpoint in endpoints:
            endpoint_id = endpoint["VpcEndpointId"]
            print(f"Deleting VPC Endpoint {endpoint_id}...")

            try:
                ec2.delete_vpc_endpoints(VpcEndpointIds=[endpoint_id])
                print(f"Successfully deleted VPC Endpoint {endpoint_id}")
            except ClientError as e:
                print(f"Error deleting VPC Endpoint {endpoint_id}: {str(e)}")
                sys.exit(1)

    except Exception as e:
        print(f"Error during VPC Endpoint cleanup: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    cleanup_vpc_endpoint()
