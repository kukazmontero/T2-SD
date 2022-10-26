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
const producer_ventas = kafka.producer();

const consumer1 = kafka.consumer({ groupId: 'grupo sexo1' })

let stock = []


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
  if(nuevousuario.Premium === "Si"){
    file.premium.push(nuevousuario)
    console.log(file.premium)
  }
  if(nuevousuario.Premium === "No"){
    file.nopremium.push(nuevousuario)
    console.log(file.nopremium)
  }
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
      file.miembros.push(file.premium[numero])
      var deletefile = file.premium.splice(numero)
      res.send({
        mensaje: "Usuario aceptado"
      })
    }
    if(aceptado === "No") {
      console.log(file.premium[numero]) 
      var deletefile = file.premium.splice(numero)
      console.log(file.premium)
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
      file.miembros.push(file.nopremium[numero])
      var deletefile = file.nopremium.splice(numero)
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
app.post("/venta", async (req, res) => {
  const fileName = './venta.json';
  const file = require(fileName);
  let nuevaventa = req.body

  file.registro.push(nuevaventa)
  //console.log(JSON.stringify(file));

  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    //console.log(JSON.stringify(file));
    });

  if(parseInt(nuevaventa.StockRestante) <= 20){
    stock.push(nuevaventa)
    console.log("HOLA")
  }

  //if(stock.length == 5){
    console.log("Poco stock")
      console.log(stock)
      console.log("\n")
      await producer_ventas.connect()
      await producer_ventas.send({
      topic: 'topico',
      messages: [
          { key: 'ventas', value: JSON.stringify(stock), partition: 0},
        ],
      }
      )
      //stock.length = 0
    //}

  res.send({
    mensaje: "Venta Registrada con Exito"
  })

})

app.get('/miembros', async (req, res) => {
  const fileName = './users.json';
  const file = require(fileName);
  console.log(file.miembros)
  res.send({
    mensaje: "Lista miembros"
  })
})
app.get('/ventas', async (req, res) => {
  const fileName = './venta.json';
  const file = require(fileName);
  console.log(file.registro)
  res.send({
    mensaje: "Registro Ventas"
  })
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

};

app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
  consumidor();
});