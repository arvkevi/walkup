import os
import sys
import requests
import boto3
from botocore.exceptions import ClientError


def get_runner_ip():
    """Get the current runner's IP address."""
    try:
        # Use ipify.org to get the public IP
        response = requests.get("https://api.ipify.org")
        response.raise_for_status()
        return response.text.strip()
    except Exception as e:
        print(f"Error getting runner IP: {e}")
        return None


def configure_rds_security():
    """Configure RDS security group for GitHub Actions runner."""
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
        postgres_rules = [
            rule
            for rule in existing_rules
            if rule.get("FromPort") == 5432 and rule.get("ToPort") == 5432
        ]
        if postgres_rules:
            print("Removing existing PostgreSQL rules...")
            try:
                ec2.revoke_security_group_ingress(
                    GroupId=security_group_id, IpPermissions=postgres_rules
                )
                print("Successfully removed existing rules")
            except ClientError as e:
                if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                    print(f"Error removing existing rules: {e}")
                    return False

        # Get runner's IP
        runner_ip = get_runner_ip()
        if not runner_ip:
            print("Could not determine runner IP address")
            return False

        print(f"Adding rule for runner IP: {runner_ip}")

        # Add rule for runner's IP
        try:
            ec2.authorize_security_group_ingress(
                GroupId=security_group_id,
                IpPermissions=[
                    {
                        "FromPort": 5432,
                        "ToPort": 5432,
                        "IpProtocol": "tcp",
                        "IpRanges": [{"CidrIp": f"{runner_ip}/32"}],
                    }
                ],
            )
            print("Successfully added rule for runner IP")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
                print("Rule already exists for this IP")
                return True
            print(f"Error adding rule: {e}")
            return False

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
