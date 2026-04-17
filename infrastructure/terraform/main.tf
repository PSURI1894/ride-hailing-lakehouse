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

variable "lakehouse_bucket_name" {
  description = "Optional fixed bucket name for the lakehouse. Leave null to let Terraform generate one."
  type        = string
  default     = null
}

variable "allow_bucket_destroy" {
  description = "Set to true only when you explicitly want Terraform to delete the bucket and every object inside it."
  type        = bool
  default     = false
}

# 1. Provide an S3 Bucket to act as our Lakehouse storage (replaces MinIO)
resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.lakehouse_bucket_name
  bucket_prefix = var.lakehouse_bucket_name == null ? "ride-hailing-lakehouse-" : null
  force_destroy = var.allow_bucket_destroy

  lifecycle {
    prevent_destroy = true
  }
}

# Secure the bucket by blocking public access
resource "aws_s3_bucket_public_access_block" "lakehouse_privacy" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lakehouse_versioning" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Create a Security Group to allow SSH temporarily
resource "aws_security_group" "pipeline_sg" {
  name_prefix = "lakehouse_pipeline_sg_"
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
  key_name_prefix = "lakehouse-key-"
  public_key      = var.public_key
}

# Create an IAM Role for the EC2 Instance
resource "aws_iam_role" "pipeline_role" {
  name_prefix = "lakehouse_role_"

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
  name_prefix = "lakehouse_s3_"
  role        = aws_iam_role.pipeline_role.id

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
  name_prefix = "lakehouse_profile_"
  role        = aws_iam_role.pipeline_role.name
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

  # Increase root volume size for Docker builds (20GB is still Free Tier)
  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    delete_on_termination = true
  }

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
