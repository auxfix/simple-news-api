const express = require('express');
const app = express();
const { Kafka } = require('kafkajs');
const http = require('http');
const socketIO = require('socket.io');
const cors = require('cors');
const dotenv = require('dotenv');

let consumer = null;
let producer = null;

async function getBusHandlers() {
    if(!!producer && !!consumer) {
        return await Promise.resolve([producer, consumer]);
    }

    const kafka = new Kafka({
        clientId: 'news-app',
        brokers: [process.env.KAFKA_BROKER],
        ssl: true,
        sasl: {
          mechanism: 'SCRAM-SHA-512',
          username: process.env.KAFKA_USER,
          password: process.env.KAFKA_PASSWORD
        },
    })
    
    producer = kafka.producer();
    consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID })
    
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: process.env.KAFKA_MSG_TOPIC, fromBeginning: true });

    return [producer, consumer];

}


dotenv.config();

app.use(cors());

const server = http.createServer(app);
const io = socketIO(server,{
    cors: {
      origin: '*',
    }
});

// Serve static files from the 'public' directory
app.use(express.static('public'));

// Socket.IO event handlers
io.on('connection', async (socket) => {
    console.log('Client connected');

    const [_, consumer]  = await getBusHandlers();

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const msg = message.value.toString()
            console.log(`Server::Socket <-- Kafka::${msg}`);
            io.emit('inform_client', msg); 
        },
    })
    
    socket.on('inform_server', async (msg) => {
        console.log(`Server::Socket --> Kafka::${msg}`);

        const [producer] = await getBusHandlers();

        await producer.send({
            topic: process.env.KAFKA_MSG_TOPIC,
            messages: [
              { value: msg },
            ],
        })
    });
});

app.get('/health', (req, res) => {
    res.status(200).send('Server is running and healthy!');
});


const PORT = process.env.PORT || 3002;
server.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});