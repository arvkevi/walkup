import os
import sys
import boto3
from botocore.exceptions import ClientError


def get_vpc_id():
    """Get the VPC ID from environment variable."""
    vpc_id = os.environ.get("VPC_ID")
    if not vpc_id:
        print("Error: VPC_ID environment variable not set")
        sys.exit(1)
    return vpc_id


def create_security_group(ec2, vpc_id):
    """Create a security group for the VPC Endpoint."""
    try:
        sg_name = "github-actions-rds-endpoint-sg"
        sg_desc = "Security group for GitHub Actions RDS VPC Endpoint"

        # Check if security group already exists
        existing_sgs = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [sg_name]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )["SecurityGroups"]

        if existing_sgs:
            print(f"Security group {sg_name} already exists")
            return existing_sgs[0]["GroupId"]

        # Create new security group
        response = ec2.create_security_group(
            GroupName=sg_name, Description=sg_desc, VpcId=vpc_id
        )
        sg_id = response["GroupId"]

        # Add inbound rule for PostgreSQL
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                }
            ],
        )

        print(f"Created security group {sg_name} with ID {sg_id}")
        return sg_id

    except ClientError as e:
        print(f"Error creating security group: {e}")
        return None


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

        # Create security group
        sg_id = create_security_group(ec2, vpc_id)
        if not sg_id:
            return False

        # Check if endpoint already exists
        service_name = f"com.amazonaws.{os.environ['AWS_REGION']}.rds"
        existing_endpoints = ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": [service_name]},
            ]
        )["VpcEndpoints"]

        if existing_endpoints:
            print("VPC Endpoint already exists")
            return True

        # Create VPC Endpoint
        try:
            response = ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName=service_name,
                SubnetIds=[subnet_id],
                SecurityGroupIds=[sg_id],
                VpcEndpointType="Interface",
                PrivateDnsEnabled=False,  # Disable private DNS since it's already configured
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
            endpoint_id = response["VpcEndpoint"]["VpcEndpointId"]
            print(f"Created VPC Endpoint: {endpoint_id}")

            # Wait for endpoint to be available
            print("Waiting for VPC Endpoint to be available...")
            waiter = ec2.get_waiter("vpc_endpoint")
            waiter.wait(
                VpcEndpointIds=[endpoint_id],
                Filters=[{"Name": "state", "Values": ["available"]}],
            )
            print("VPC Endpoint is now available")

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
        "VPC_ID",
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        print("Missing required environment variables:", ", ".join(missing))
        sys.exit(1)

    if not configure_vpc_endpoint():
        sys.exit(1)
