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
    mensaje: "enviado"
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
/*await consumer1.run({
  partitionsConsumedConcurrently: 2,
  eachMessage: async ({ topic, partition, message }) => {
    msgNumber++
    kafka.logger().info('Incoming message', {
      topic,
      partition,
      offset: message.offset,
      timestamp: message.timestamp,
      headers: Object.keys(message.headers).reduce(
        (headers, key) => ({
          ...headers,
          [key]: message.headers[key].toString(),
        }),
        {}
      ),
      key: message.key.toString(),
      value: message.value.toString(),
      msgNumber,
    })

    kafka.logger().info('Now sleeping for 5000ms')
    await sleep(5000)
    kafka.logger().info('Finished sleeping')
  },
})*/

  /*await consumer1.run({
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
    partitionsConsumedConcurrently: 2,
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            partition,
            key: message.key.toString(),
            value: message.value.toString(),
            headers: message.headers,
        })
    },
})*/
  res.send({
    mensaje: "recibido"
  })
  });



  //res.send("Mensaje enviado con exito puto");


app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
});