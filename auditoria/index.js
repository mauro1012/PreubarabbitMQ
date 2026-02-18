require('dotenv').config();
const amqp = require('amqplib');
const { MongoClient } = require('mongodb');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://user:password@rabbitmq';
const MONGO_URL = process.env.MONGO_URL || 'mongodb://mongodb:27017';

async function iniciarAuditoria() {
    let dbClient;
    let conectado = false;

    // Bucle de reintento para MongoDB
    while (!conectado) {
        try {
            dbClient = await MongoClient.connect(MONGO_URL);
            conectado = true;
            console.log('✅ Conectado a MongoDB con éxito');
        } catch (error) {
            console.error('❌ Error conectando a MongoDB, reintentando en 5s...');
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }

    const db = dbClient.db('db_auditoria');
    const coleccion = db.collection('logs');

    try {
        const connection = await amqp.connect(RABBIT_URL);
        const channel = await connection.createChannel();

        const exchange = 'logs_exchange';
        await channel.assertExchange(exchange, 'fanout', { durable: false });

        // Creamos una cola temporal que se destruye al cerrar
        const q = await channel.assertQueue('', { exclusive: true });
        
        // Unimos la cola al exchange (Binding)
        await channel.bindQueue(q.queue, exchange, '');

        console.log('🚀 Esperando mensajes en RabbitMQ...');

        channel.consume(q.queue, async (msg) => {
            if (msg !== null) {
                const contenido = JSON.parse(msg.content.toString());
                
                // Guardar en MongoDB
                const resultado = await coleccion.insertOne({
                    ...contenido,
                    procesado_el: new Date()
                });

                console.log(`📥 Log guardado en Mongo con ID: ${resultado.insertedId}`);
                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('❌ Error en RabbitMQ:', error);
    }
}

iniciarAuditoria();