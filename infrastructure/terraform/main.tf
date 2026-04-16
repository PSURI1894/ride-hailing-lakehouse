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

# Create a Security Group to allow SSH temporarily
resource "aws_security_group" "pipeline_sg" {
  name        = "lakehouse_pipeline_sg"
  description = "Allow SSH inbound traffic"

  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

variable "public_key" {
  description = "Ephemeral Public Key for SSH injected by GitHub Actions"
  type        = string
}

resource "aws_key_pair" "ephemeral_key" {
  key_name   = "lakehouse-ephemeral-key"
  public_key = var.public_key
}

# Create an IAM Role for the EC2 Instance
resource "aws_iam_role" "pipeline_role" {
  name = "lakehouse_pipeline_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# Attach S3 Full Access to the Role (Optimized for the specific bucket)
resource "aws_iam_role_policy" "s3_access" {
  name = "lakehouse_s3_access"
  role = aws_iam_role.pipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "s3:*"
        Effect = "Allow"
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}",
          "${aws_s3_bucket.lakehouse.arn}/*"
        ]
      }
    ]
  })
}

# Create an Instance Profile to attach to the EC2
resource "aws_iam_instance_profile" "pipeline_profile" {
  name = "lakehouse_pipeline_profile"
  role = aws_iam_role.pipeline_role.name
}

# 2. Ephemeral Compute Instance (Placeholder for Phase 6 Pipeline Execution)
# This prevents 24/7 charges by only spinning up via CI/CD, running the Spark job, and terminating.
resource "aws_instance" "pipeline_runner" {
  ami           = "ami-0c7217cdde317cfec" # Ubuntu 22.04 LTS us-east-1 
  instance_type = "t2.micro"              # AWS Free Tier eligible
  key_name      = aws_key_pair.ephemeral_key.key_name
  vpc_security_group_ids = [aws_security_group.pipeline_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.pipeline_profile.name

  # Make sure we get a public IP
  associate_public_ip_address = true

  tags = {
    Name = "Lakehouse-Pipeline-Runner"
  }
}

output "instance_public_ip" {
  value = aws_instance.pipeline_runner.public_ip
}

output "s3_bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}
