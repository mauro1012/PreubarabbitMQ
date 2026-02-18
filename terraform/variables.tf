
# VARIABLES DE CONEXIÓN AWS (ACADEMY)

variable "aws_region" {
  description = "Región de AWS donde se desplegará la infraestructura de RabbitMQ"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key" {
  description = "Access Key de AWS Academy"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "Secret Key de AWS Academy"
  type        = string
  sensitive   = true
}

variable "aws_session_token" {
  description = "Session Token temporal proporcionado por AWS Academy"
  type        = string
  sensitive   = true
}


# CONFIGURACIÓN DE INSTANCIA Y ACCESO

variable "ssh_key_name" {
  description = "Nombre de la llave SSH (.pem) ya existente en AWS para acceder a la EC2"
  type        = string
  default     = "vockey" 
}

variable "instance_type" {
  description = "Tipo de instancia EC2. Se recomienda t3.medium para RabbitMQ + MongoDB"
  type        = string
  default     = "t3.medium"
  
  validation {
    condition     = contains(["t2.medium", "t3.medium", "t3.large"], var.instance_type)
    error_message = "La instancia debe ser al menos medium para soportar el cluster de RabbitMQ y MongoDB."
  }
}


# VARIABLES DE DOCKER HUB

variable "docker_user" {
  description = "Usuario de Docker Hub para descargar las imágenes de los microservicios"
  type        = string
  
  validation {
    condition     = length(var.docker_user) > 0
    error_message = "El nombre de usuario de Docker Hub no puede estar vacío."
  }
}


# CONFIGURACIÓN DE RABBITMQ

variable "rabbit_user" {
  description = "Usuario administrador para el panel de RabbitMQ"
  type        = string
  default     = "user"
}

variable "rabbit_password" {
  description = "Contraseña para el panel de RabbitMQ"
  type        = string
  default     = "password"
  sensitive   = true
}