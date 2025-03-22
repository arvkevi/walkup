import boto3
import requests
import os
from dotenv import load_dotenv


def get_my_ip():
    """Get your current public IP address."""
    response = requests.get("https://api.ipify.org?format=json")
    return response.json()["ip"]


def configure_rds_security():
    """Configure RDS security group with proper inbound rules."""
    load_dotenv()

    # AWS credentials from environment variables
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    security_group_id = os.environ.get("RDS_SECURITY_GROUP")
    vpc_id = os.environ.get("VPC_ID")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not all([aws_access_key, aws_secret_key, security_group_id, vpc_id]):
        print("Missing required environment variables.")
        return

    # Initialize AWS client
    ec2 = boto3.client(
        "ec2",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )

    try:
        # Get VPC CIDR block
        vpc_response = ec2.describe_vpcs(VpcIds=[vpc_id])
        vpc_cidr = vpc_response["Vpcs"][0]["CidrBlock"]

        # Get your current IP
        my_ip = get_my_ip()

        # Remove existing rules
        response = ec2.describe_security_groups(GroupIds=[security_group_id])
        existing_rules = response["SecurityGroups"][0]["IpPermissions"]
        if existing_rules:
            ec2.revoke_security_group_ingress(
                GroupId=security_group_id, IpPermissions=existing_rules
            )

        # Add new rules
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpRanges": [
                        {
                            "CidrIp": f"{my_ip}/32",
                            "Description": "Local development access",
                        },
                        {
                            "CidrIp": vpc_cidr,
                            "Description": "VPC access for GitHub Actions",
                        },
                    ],
                }
            ],
        )

        print("\nSecurity group updated successfully!")
        print(f"\nCurrent inbound rules:")
        print(f"1. Local development access: {my_ip}/32")
        print(f"2. VPC access: {vpc_cidr}")
        print("\nMake sure to update these rules if:")
        print("- Your local IP address changes")
        print("- You need to add access for other developers")
        print("- You're deploying to other environments")

    except Exception as e:
        print(f"Error configuring security group: {e}")


if __name__ == "__main__":
    configure_rds_security()
