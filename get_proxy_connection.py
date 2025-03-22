import os
import sys
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse


def get_proxy_endpoint():
    """Get the RDS Proxy endpoint."""
    try:
        rds = boto3.client(
            "rds",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
        )

        proxy_name = "github-actions-rds-proxy"
        response = rds.describe_db_proxies(DBProxyName=proxy_name)

        if not response["DBProxies"]:
            print(f"RDS Proxy {proxy_name} not found")
            return None

        return response["DBProxies"][0]["Endpoint"]

    except ClientError as e:
        print(f"Error getting proxy endpoint: {e}")
        return None


def get_proxy_connection_string():
    """Get database connection string using IAM authentication."""
    try:
        # Parse original connection URI to get database name
        conn_uri = os.environ["CONNECTION_URI"]
        parsed = urlparse(conn_uri)
        db_name = parsed.path[1:]  # Remove leading '/'

        # Get proxy endpoint
        proxy_endpoint = get_proxy_endpoint()
        if not proxy_endpoint:
            return None

        # Get IAM authentication token
        rds = boto3.client("rds")
        auth_token = rds.generate_db_auth_token(
            DBHostname=proxy_endpoint,
            Port=5432,
            DBUsername=parsed.username,
            Region=os.environ["AWS_REGION"],
        )

        # Construct new connection string
        proxy_uri = (
            f"postgresql://{parsed.username}:{auth_token}@"
            f"{proxy_endpoint}:5432/{db_name}"
        )

        print("\nRDS Proxy connection string (valid for 15 minutes):")
        print(f"postgresql://{parsed.username}:****@{proxy_endpoint}:5432/{db_name}")
        print("\nTo use this connection string:")
        print("1. Set the PGPASSWORD environment variable to the auth token")
        print("2. Use psql or your application to connect\n")

        return proxy_uri

    except Exception as e:
        print(f"Error generating connection string: {e}")
        return None


if __name__ == "__main__":
    if not get_proxy_connection_string():
        sys.exit(1)
