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
      { value: 'Rodrigo eres la persona mas negra que eh conocido en mi vida' },
    ],
  })

  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Benjamin bacallo kl' },
    ],
  })
  res.send({
    mensaje: "enviado"
  })
});
  // Consuming
  app.get("/rx", async (req,res) => {

  await consumer1.connect()
  await consumer1.subscribe({ topic: 'test-topic', fromBeginning: false })

  await consumer1.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        //partition,
        //offset: message.offset,
        Consumer:1,
        value: message.value.toString(),
      })
    },
  })

  await   consumer2.connect()
  await consumer2.subscribe({ topic: 'test-topic', fromBeginning: false })

  console.log("Consumidor 2:")
  await consumer2.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        //partition,
        //offset: message.offset,
        Consumer:2,
        value: message.value.toString(),
      })
    },
  })
  res.send({
    mensaje: "recibido"
  })
  });



  //res.send("Mensaje enviado con exito puto");


app.listen(port, () => {
  console.log(`Consumidor CORRIENDO`);
});