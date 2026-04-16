terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Note: A real deployment would use a remote backend like s3
  # backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

# 1. Provide an S3 Bucket to act as our Lakehouse storage (replaces MinIO)
resource "aws_s3_bucket" "lakehouse" {
  bucket_prefix = "ride-hailing-lakehouse-"
  force_destroy = true # Allows easy tear-down
}

# Secure the bucket by blocking public access
resource "aws_s3_bucket_public_access_block" "lakehouse_privacy" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 2. Ephemeral Compute Instance (Placeholder for Phase 6 Pipeline Execution)
# This prevents 24/7 charges by only spinning up via CI/CD, running the Spark job, and terminating.
resource "aws_instance" "pipeline_runner" {
  ami           = "ami-0c7217cdde317cfec" # Ubuntu 22.04 LTS us-east-1 (check validity)
  instance_type = "t3.large"              # Provides 8GB RAM, enough to run our docker-compose pipeline
  
  # We associate an IAM profile so it can access the S3 bucket
  iam_instance_profile = aws_iam_instance_profile.pipeline_profile.name

  tags = {
    Name = "Lakehouse-Pipeline-Runner"
  }
}

# (IAM Roles and Networking to be added later as CI expands)
