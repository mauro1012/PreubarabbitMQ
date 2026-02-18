require('dotenv').config();
const express = require('express');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://user:password@rabbitmq';

let channel;

// Función para conectar a RabbitMQ
async function conectarRabbit() {
    try {
        const connection = await amqp.connect(RABBIT_URL);
        channel = await connection.createChannel();
        
        // Usamos un Exchange de tipo 'fanout'
        const exchange = 'logs_exchange';
        await channel.assertExchange(exchange, 'fanout', { durable: false });
        
        console.log('Gateway conectado a RabbitMQ');
    } catch (error) {
        console.error('Error conectando a RabbitMQ, reintentando...', error);
        setTimeout(conectarRabbit, 5000);
    }
}

// Endpoint para recibir eventos
app.post('/enviar-evento', async (req, res) => {
    const { usuario, accion, detalle } = req.body;

    if (!usuario || !accion) {
        return res.status(400).send({ error: "Faltan datos: usuario y accion son obligatorios" });
    }

    const payload = {
        usuario,
        accion,
        detalle: detalle || "Sin detalles",
        fecha: new Date()
    };

    // Publicamos el mensaje en el exchange
    const exchange = 'logs_exchange';
    channel.publish(exchange, '', Buffer.from(JSON.stringify(payload)));

    console.log('📤 Evento enviado a RabbitMQ:', payload);
    res.status(200).send({ 
        mensaje: "Evento enviado a la cola de RabbitMQ",
        data: payload 
    });
});

// Health check para el balanceador de AWS
app.get('/health', (req, res) => {
    res.status(200).send('OK');
});

app.listen(PORT, () => {
    console.log(` Gateway RabbitMQ escuchando en puerto ${PORT}`);
    conectarRabbit();
});