terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  # all the required configurations were set using aws configure
}

# resource tells which resource we'll be creating
resource "aws_instance" "my_first_server" {
  ami           = "ami-03cd80cfebcbb4481"
  instance_type = "t2.micro"
  tags = {
    Name = "my_first_server"
  }
}

resource "aws_s3_bucket" "my_first_bucket" {
  bucket = "my-first-bucket-unique13254"
  tags = {
    Name        = "Terraform_made_bucket"
  }
}

# output block allows to define values to be displayed after applying
output "instance_ip" {
  value = aws_instance.my_first_server.public_ip
}