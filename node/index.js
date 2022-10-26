const express = require("express");
const cors = require("cors");
const { Kafka } = require('kafkajs')
const fs = require('fs')


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



app.get("/listapremium", async (req, res) => {
  // Producing
  let jsonData = require('./users.json');
  for(i in jsonData.premium){
      jsonData.premium[i].numerolista = i;
      await producer.connect()
      await producer.send({
        topic: 'test-topic',
        messages: [
          { key: 'Premium', value: JSON.stringify( jsonData.premium[i]), partition: 0, },
        ]

      })
  }
  await producer.disconnect()
  res.send({
    mensaje: "Lista Premium"
  })
})
app.get("/lista", async (req, res) => {
  // Producing
  let jsonData = require('./users.json');
  for(i in jsonData.nopremium){
    jsonData.nopremium[i].numerolista = i;
    await producer.connect()
      await producer.send({
        topic: 'test-topic',
        messages: [
          { key: 'NoPremium', value: JSON.stringify(jsonData.nopremium[i]), partition: 1 , },
        ],
      }
      )
  }
  await producer.disconnect()
  res.send({
    mensaje: "Lista Publicada"
  })
})

app.post("/registro", async (req, res) => {
  // Producing
  const fileName = './users.json';
  const file = require(fileName);
  let nuevousuario = req.body
  file.premium.push(nuevousuario)
  console.log(file.premium)
  /*let jsonData = require('./users.json');
  */
  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    console.log(JSON.stringify(file));
    console.log('Guardando en ' + fileName);
  });

  await producer.disconnect()
  res.send({
    mensaje: "Usuario Publicado"
  })
})
app.post("/aceptar", async (req, res) => {
  // Producing
  const fileName = './users.json';
  const file = require(fileName);
  let aceptado = req.query.Aceptado
  let numero = parseInt(req.query.NumeroLista) 
  let premium = req.query.UsuarioPremium
  //console.log(premium)
  if(premium === "Si") {
    if(aceptado === "Si") {
      console.log("Si")
      res.send({
        mensaje: "Usuario aceptado"
      })
    }
    if(aceptado === "No") {
      console.log("No")
      console.log(file.nopremium[numero]) 
      var deletefile = file.nopremium.splice(numero)
      console.log(file.nopremium)
      res.send({
        mensaje: "Usuario No aceptado"
      })
    }
    /*delete file.premium[numero]
    console.log(file.premium)*/
  }
  if(premium === "No") {
    if(aceptado === "Si") {
      console.log("Si")
      res.send({
        mensaje: "Usuario aceptado"
      })
    }
    if(aceptado === "No") {
      console.log("No")
      console.log(file.nopremium[numero]) 
      var deletefile = file.nopremium.splice(numero)
      console.log(file.nopremium)
      res.send({
        mensaje: "Usuario No aceptado"
      })
    }
    
  }

  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    console.log(JSON.stringify(file));
    console.log('Guardando en ' + fileName);
  });

  await producer.disconnect()
  
})

async function consumidor() {
  consumer1.connect()
  await consumer1.subscribe({ topic: 'test-topic', fromBeginning: true })
  await consumer1.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
        console.log({
            key: message.key.toString(),
            value:JSON.parse(message.value.toString()),
            partition,
        })
    },
})

  res.send({
    mensaje: "recibido"
  })
};

app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
  consumidor();
});