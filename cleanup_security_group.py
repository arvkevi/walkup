import os
import boto3
import sys
from botocore.exceptions import ClientError


def cleanup_security_group():
    """Remove security group rules that were added for GitHub Actions."""
    try:
        # Get required environment variables
        security_group_id = os.getenv("RDS_SECURITY_GROUP")
        if not security_group_id:
            sys.stdout.write("Error: RDS_SECURITY_GROUP environment variable not set\n")
            sys.exit(1)

        # Initialize AWS client
        ec2 = boto3.client("ec2")

        # Get existing rules
        try:
            response = ec2.describe_security_groups(GroupIds=[security_group_id])
            security_group = response["SecurityGroups"][0]
            existing_rules = security_group.get("IpPermissions", [])
        except ClientError as e:
            sys.stdout.write(f"Error getting security group rules: {str(e)}\n")
            sys.exit(1)

        # Find and remove rules for GitHub Actions
        rules_to_remove = []
        for rule in existing_rules:
            if (
                rule.get("FromPort") == 5432
                and rule.get("ToPort") == 5432
                and rule.get("IpProtocol") == "tcp"
                and "IpRanges" in rule
            ):
                for ip_range in rule["IpRanges"]:
                    if ip_range.get("Description", "").startswith("GitHub Actions"):
                        rules_to_remove.append(rule)
                        break

        if not rules_to_remove:
            sys.stdout.write("No GitHub Actions rules found to remove\n")
            return

        # Remove the rules
        try:
            ec2.revoke_security_group_ingress(
                GroupId=security_group_id, IpPermissions=rules_to_remove
            )
            sys.stdout.write(
                f"Successfully removed {len(rules_to_remove)} security group rules\n"
            )
        except ClientError as e:
            sys.stdout.write(f"Error removing security group rules: {str(e)}\n")
            sys.exit(1)

    except Exception as e:
        sys.stdout.write(f"Unexpected error: {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    cleanup_security_group()
