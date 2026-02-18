require('dotenv').config();
const amqp = require('amqplib');
const { MongoClient } = require('mongodb');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://user:password@rabbitmq';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017';
const DB_NAME = 'db_auditoria';

async function iniciarAuditoria() {
    try {
        // Conexión a MongoDB
        const client = await MongoClient.connect(MONGO_URL);
        const db = client.db(DB_NAME);
        const logsCol = db.collection('logs');
        console.log('Conectado a MongoDB');

        // Conexión a RabbitMQ
        const connection = await amqp.connect(RABBIT_URL);
        const channel = await connection.createChannel();

        const exchange = 'logs_exchange';
        await channel.assertExchange(exchange, 'fanout', { durable: false });

        // Crear cola temporal automática
        const q = await channel.assertQueue('', { exclusive: true });
        console.log(` Esperando mensajes en cola: ${q.queue}`);

        // Unir la cola al exchange
        await channel.bindQueue(q.queue, exchange, '');

        channel.consume(q.queue, async (msg) => {
            if (msg !== null) {
                const contenido = JSON.parse(msg.content.toString());
                console.log(' Mensaje recibido:', contenido);

                // Guardar en MongoDB
                await logsCol.insertOne({
                    ...contenido,
                    procesado_el: new Date()
                });
                console.log('Log guardado en Mongo');
                
                channel.ack(msg);
            }
        });

    } catch (error) {
        console.error(' Error en Auditoría, reintentando en 5s...', error.message);
        setTimeout(iniciarAuditoria, 5000);
    }
}

iniciarAuditoria();