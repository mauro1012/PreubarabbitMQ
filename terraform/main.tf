# 1. PROVEEDOR Y CONFIGURACIÓN
provider "aws" {
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket  = "examen-suple-rabbitMQ-2026-mauro"
    key     = "proyecto-rabbitmq-final/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

# 2. RED POR DEFECTO Y FILTRO DE ZONAS
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Generador de sufijo para nombres únicos
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# 3. SEGURIDAD (Security Groups)
resource "aws_security_group" "sg_rabbit_alb" {
  name        = "sg-rabbitmq-alb-${random_string.suffix.result}"
  description = "Acceso publico al balanceador"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 80
    to_port     = 80
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

resource "aws_security_group" "sg_rabbit_nodes" {
  name        = "sg-rabbitmq-nodes-${random_string.suffix.result}"
  description = "Acceso interno para RabbitMQ y App"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 3000
    to_port         = 3000
    protocol        = "tcp"
    security_groups = [aws_security_group.sg_rabbit_alb.id]
  }

  ingress {
    from_port   = 15672 # Puerto del Panel de Control de RabbitMQ
    to_port     = 15672
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
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

# 4. BALANCEADOR DE CARGA (ALB)
resource "aws_lb" "rabbit_alb" {
  name               = "alb-rabbitmq-mauro"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.sg_rabbit_alb.id]
  subnets            = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "rabbit_tg" {
  name     = "tg-rabbit-mauro"
  port     = 3000
  protocol = "HTTP"
  vpc_id   = data.aws_vpc.default.id

  health_check {
    path                = "/health"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "rabbit_listener" {
  load_balancer_arn = aws_lb.rabbit_alb.arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.rabbit_tg.arn
  }
}

# 5. LANZAMIENTO (EC2 con RabbitMQ y MongoDB)
resource "aws_launch_template" "rabbit_lt" {
  name_prefix   = "lt-rabbitmq-"
  image_id      = "ami-0c7217cdde317cfec" 
  instance_type = "t3.medium"
  key_name      = var.ssh_key_name

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [aws_security_group.sg_rabbit_nodes.id]
  }

  user_data = base64encode(<<-EOF
              #!/bin/bash
              sudo apt update -y
              sudo apt install -y docker.io docker-compose
              sudo systemctl start docker

              mkdir -p /home/ubuntu/app && cd /home/ubuntu/app

              cat <<EOT > docker-compose.yml
              version: '3.8'
              services:
                rabbitmq:
                  image: rabbitmq:3-management
                  container_name: rabbitmq_broker
                  ports:
                    - "5672:5672"
                    - "15672:15672"
                  environment:
                    RABBITMQ_DEFAULT_USER: user
                    RABBITMQ_DEFAULT_PASS: password

                mongodb:
                  image: mongo:latest
                  container_name: mongodb_logs
                  volumes:
                    - mongo_data:/data/db

                gateway:
                  image: ${var.docker_user}/gateway-rabbit:latest
                  container_name: gateway_producer
                  ports:
                    - "3000:3000"
                  environment:
                    - RABBIT_URL=amqp://user:password@rabbitmq
                  depends_on:
                    - rabbitmq

                auditoria:
                  image: ${var.docker_user}/auditoria-rabbit:latest
                  container_name: auditoria_consumer
                  environment:
                    - RABBIT_URL=amqp://user:password@rabbitmq
                    - MONGO_URL=mongodb://mongodb:27017
                  depends_on:
                    - rabbitmq
                    - mongodb

              volumes:
                mongo_data:
              EOT

              sudo docker-compose up -d
              EOF
  )
}

# 6. ESCALAMIENTO (ASG)
resource "aws_autoscaling_group" "rabbit_asg" {
  name                = "asg-rabbitmq-mauro"
  desired_capacity    = 1
  max_size            = 1
  min_size            = 1
  target_group_arns   = [aws_lb_target_group.rabbit_tg.arn]
  vpc_zone_identifier = data.aws_subnets.default.ids

  launch_template {
    id      = aws_launch_template.rabbit_lt.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "EC2-RabbitMQ-Mauro"
    propagate_at_launch = true
  }
}

# 7. SALIDAS (Outputs)
output "URL_POSTMAN_API" {
  value = "http://${aws_lb.rabbit_alb.dns_name}/enviar-evento"
}

output "PANEL_CONTROL_RABBITMQ" {
  value = "http://${aws_lb.rabbit_alb.dns_name}:15672 (Usar IP de la instancia si el ALB no redirige este puerto)"
}