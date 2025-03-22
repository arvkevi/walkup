import os
import sys
import json
import requests
import boto3
from botocore.exceptions import ClientError


def get_github_actions_ip_ranges():
    """Get GitHub Actions IP ranges."""
    try:
        response = requests.get("https://api.github.com/meta")
        response.raise_for_status()
        return response.json().get("actions", [])
    except Exception as e:
        print(f"Error fetching GitHub Actions IP ranges: {e}")
        return []


def configure_rds_security():
    """Configure RDS security group for GitHub Actions."""
    try:
        # Create EC2 client
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
        )

        security_group_id = os.environ["RDS_SECURITY_GROUP"]

        # Get current security group rules
        response = ec2.describe_security_groups(GroupIds=[security_group_id])
        existing_rules = response["SecurityGroups"][0]["IpPermissions"]

        # Remove existing PostgreSQL rules
        for rule in existing_rules:
            if rule.get("FromPort") == 5432 and rule.get("ToPort") == 5432:
                try:
                    ec2.revoke_security_group_ingress(
                        GroupId=security_group_id, IpPermissions=[rule]
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                        raise

        # Get GitHub Actions IP ranges
        ip_ranges = get_github_actions_ip_ranges()
        if not ip_ranges:
            print("No GitHub Actions IP ranges found")
            sys.exit(1)

        # Add rules for GitHub Actions IPs
        for i in range(0, len(ip_ranges), 50):  # Process in batches of 50
            batch = ip_ranges[i : i + 50]
            try:
                ec2.authorize_security_group_ingress(
                    GroupId=security_group_id,
                    IpPermissions=[
                        {
                            "FromPort": 5432,
                            "ToPort": 5432,
                            "IpProtocol": "tcp",
                            "IpRanges": [{"CidrIp": ip} for ip in batch],
                        }
                    ],
                )
                print(f"Added rules for IPs {i+1} to {i+len(batch)}")
            except ClientError as e:
                if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                    raise

        print("Successfully configured security group")
        return True

    except Exception as e:
        print(f"Error configuring security group: {e}")
        return False


if __name__ == "__main__":
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "RDS_SECURITY_GROUP",
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        print("Missing required environment variables:", ", ".join(missing))
        sys.exit(1)

    if not configure_rds_security():
        sys.exit(1)
