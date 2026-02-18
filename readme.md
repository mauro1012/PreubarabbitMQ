Este es un **README.md** profesional y estructurado que resume todo el proceso de pruebas que realizamos. Está diseñado para que cualquier persona (o tu profesor) pueda seguir el flujo de datos desde Postman hasta la base de datos.

---

# 🚀 Guía de Pruebas: Sistema de Mensajería con RabbitMQ & MongoDB

Este documento detalla los pasos para verificar la comunicación asíncrona entre el **Gateway (Productor)** y el **Servicio de Auditoría (Consumidor)** utilizando un **Exchange de tipo Fanout**.

## 1. Verificación de Infraestructura (Docker)

Antes de probar, asegúrate de que los contenedores estén corriendo correctamente en la instancia EC2.

```bash
# Listar todos los contenedores activos
sudo docker ps

# Verificar que el Consumidor esté conectado a Rabbit y Mongo
sudo docker logs auditoria_consumer

# Verificar que el Productor esté escuchando peticiones
sudo docker logs gateway_producer

```

---

## 2. Pruebas con Postman (Productor)

El **Gateway** recibe peticiones HTTP y las transforma en mensajes de RabbitMQ.

* **Método:** `POST`
* **URL:** `http://<DNS-DEL-ALB>/enviar-evento`
* **Headers:** `Content-Type: application/json`
* **Cuerpo (Body) -> raw -> JSON:**

```json
{
  "usuario": "Mauro Daniel",
  "accion": "Prueba de Integración",
  "detalle": "Mensaje enviado exitosamente a través de RabbitMQ"
}

```

> **Nota:** Al presionar **Send**, deberías recibir una respuesta `200 OK` confirmando que el evento fue enviado al Exchange.

---

## 3. Monitoreo en el Panel de RabbitMQ

Puedes visualizar el flujo de mensajes en tiempo real a través del plugin de gestión.

* **URL:** `http://<IP-PUBLICA-EC2>:15672`
* **Usuario:** `user`
* **Contraseña:** `password`
* **Pasos:**
1. Ve a la pestaña **Exchanges** y busca `logs_exchange`.
2. Observa el pico en la gráfica de **Message rate**.
3. Ve a la pestaña **Queues** y verifica que existe una cola dinámica (ej. `amq.gen-...`) con los mensajes siendo procesados.



---

## 4. Verificación de Persistencia (MongoDB)

Finalmente, comprobamos que el consumidor procesó el mensaje y lo guardó en la base de datos NoSQL.

```bash
# 1. Entrar al contenedor de MongoDB
sudo docker exec -it mongodb_logs mongosh

# 2. Cambiar a la base de datos de auditoría
use db_auditoria

# 3. Listar las colecciones (debe aparecer 'logs')
show collections

# 4. Consultar los registros guardados
db.logs.find().pretty()

```

---

## 5. Resumen del Flujo de Datos

1. **Postman** envía un JSON al **ALB (AWS)**.
2. El **ALB** redirige la carga al contenedor **Gateway**.
3. El **Gateway** publica el mensaje en el **Exchange** de RabbitMQ.
4. El **Exchange** envía una copia a todas las colas vinculadas.
5. El servicio de **Auditoría** consume el mensaje de su cola.
6. La **Auditoría** inserta el documento final en **MongoDB**.

---

**¿Te gustaría que agregue una sección de "Troubleshooting" (solución de problemas) por si falla la conexión al iniciar?**