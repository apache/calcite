# AWS Permissions Required for CloudOps Adapter

The CloudOps adapter needs read-only permissions to query AWS resources.

## Option 1: Attach AWS Managed Policies (Easiest)

Attach these AWS managed policies to the IAM user `ken`:
- `ReadOnlyAccess` - Provides read-only access to all AWS services
- OR more granular policies:
  - `AmazonEKSReadOnlyAccess` - For EKS clusters
  - `AmazonEC2ReadOnlyAccess` - For EC2 instances and VPCs
  - `IAMReadOnlyAccess` - For IAM resources
  - `AmazonRDSReadOnlyAccess` - For RDS databases
  - `AmazonEC2ContainerRegistryReadOnly` - For ECR
  - `CloudWatchReadOnlyAccess` - For CloudWatch metrics

## Option 2: Create Custom Policy

Create a new policy with this JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudOpsReadOnly",
      "Effect": "Allow",
      "Action": [
        "eks:ListClusters",
        "eks:DescribeCluster",
        "eks:ListNodegroups",
        "eks:DescribeNodegroup",
        "ec2:DescribeInstances",
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeInternetGateways",
        "ec2:DescribeNatGateways",
        "ec2:DescribeRouteTables",
        "ec2:DescribeAddresses",
        "iam:ListUsers",
        "iam:ListRoles",
        "iam:ListGroups",
        "iam:ListPolicies",
        "iam:ListInstanceProfiles",
        "iam:GetUser",
        "iam:GetRole",
        "iam:GetPolicy",
        "rds:DescribeDBInstances",
        "rds:DescribeDBClusters",
        "rds:DescribeDBSnapshots",
        "rds:ListTagsForResource",
        "ecr:DescribeRepositories",
        "ecr:ListImages",
        "ecr:GetRepositoryPolicy",
        "ecr:ListTagsForResource",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
        "s3:GetBucketEncryption",
        "s3:GetBucketTagging",
        "s3:GetBucketLifecycleConfiguration",
        "s3:GetBucketPublicAccessBlock",
        "s3:GetEncryptionConfiguration",
        "s3:ListAllMyBuckets",
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTargetGroups",
        "autoscaling:DescribeAutoScalingGroups",
        "kms:ListKeys",
        "kms:DescribeKey",
        "tag:GetResources"
      ],
      "Resource": "*"
    }
  ]
}
```

## How to Apply in AWS Console

1. Log into AWS Console with an administrator account
2. Navigate to **IAM** → **Users** → **ken**
3. Click **Add permissions** → **Attach policies directly**
4. Either:
   - Search for and select the managed policies listed above, OR
   - Click **Create policy**, paste the JSON above, name it `CloudOpsReadOnlyAccess`
5. Click **Next** → **Add permissions**

## How to Apply with AWS CLI (Requires Admin Access)

If you have admin credentials configured:

```bash
# Option 1: Attach AWS managed ReadOnlyAccess policy
aws iam attach-user-policy \
  --user-name ken \
  --policy-arn arn:aws:iam::aws:policy/ReadOnlyAccess

# Option 2: Attach individual managed policies
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/AmazonEKSReadOnlyAccess
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/AmazonEC2ReadOnlyAccess
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/IAMReadOnlyAccess
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/AmazonRDSReadOnlyAccess
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
aws iam attach-user-policy --user-name ken --policy-arn arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess
```

## Testing After Permissions Are Added

Run the CloudOps comprehensive count test again:
```bash
./gradlew :cloud-ops:test --tests "*.CloudOpsComprehensiveCountTest"
```

You should see data from AWS resources without 403 errors.
