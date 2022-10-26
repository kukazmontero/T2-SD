const express = require("express");
const cors = require("cors");
const { Kafka } = require('kafkajs')

const port = process.env.PORT || 3000;
const app = express();

app.use(cors());
app.use(express.json());

//nos conectamos a kafka
const kafka = new Kafka({
  brokers: [process.env.kafkaHost]
});

//somos producer
const producer = kafka.producer();
const consumer1 = kafka.consumer({ groupId: 'grupo sexo1' })
const consumer2 = kafka.consumer({ groupId: 'grupo sexo2' })

app.get("/", async (req, res) => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [
      { key: 'key1', value: 'hello world', partition: 0 },
      { key: 'key2', value: 'hey hey!', partition: 1 }
    ]
  })
  await producer.disconnect()

  res.send({
    mensaje: "enviadoo"
  })
})








  // Consuming
  app.get("/rx", async (req,res) => {

  await consumer1.connect()
  await consumer1.subscribe({ topic: 'test-topic', fromBeginning: true })
  
  await consumer1.run({
    partitionsConsumedConcurrently: 2,
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            partition,
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
})




  await consumer2.connect()
  await consumer2.subscribe({ topic: 'test-topic', fromBeginning: true })

  //console.log("Consumidor 2:")
  await consumer2.run({
    partitionsConsumedConcurrently: 2,
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            partition,
            key: message.key.toString(),
            value: message.value.toString(),
        })
    },
})
  res.send({
    mensaje: "recibido"
  })
  });





app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
});