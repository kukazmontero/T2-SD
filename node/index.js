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
const producer_ubicaciones = kafka.producer();

const consumer1 = kafka.consumer({ groupId: 'grupo 1' })

var stock = [];

const fileName3 = './ubicaciones.json';
const file3 = require(fileName3);


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
        mensaje: "Usuario aceptado1"
      })
    }
    if(aceptado === "No") {
      console.log(file.premium[numero]) 
      var deletefile = file.premium.splice(numero)
      console.log(file.premium)
      res.send({
        mensaje: "Usuario No aceptado2"
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
        mensaje: "Usuario aceptado3"
      })
    }
    if(aceptado === "No") {
      console.log("No")
      console.log(file.nopremium[numero]) 
      var deletefile = file.nopremium.splice(numero)
      console.log(file.nopremium)
      res.send({
        mensaje: "Usuario No aceptado4"
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

  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    });

  console.log("Registro de venta Ingresado")
  console.log(JSON.stringify(nuevaventa))
  
  // Si una venta contiene un stock <= 20, se guarda en un arreglo 
  if(parseInt(nuevaventa.StockRestante) <= 20){
    stock.push(nuevaventa)

    console.log("Poco stock")
    console.log("\n")

      await producer_ventas.connect()
      await producer_ventas.send({
      topic: 'topico',
      messages: [
          { key: 'ventas', value: JSON.stringify(stock), partition: 0},
        ],
      }) 
    
    }

    for(i in file3.registro){
      if(nuevaventa.RutMaestro.toString() === file3.registro[i].RutMaestro.toString()){
        console.log("Ubicacion de maestro ya guardada")
        
      }
      else{
      aux = {
        "RutMaestro": nuevaventa.RutMaestro,
        "Ubicacion": nuevaventa.Ubicacion
      }

      file3.registro.push(aux)
      fs.writeFile(fileName3, JSON.stringify(file3), function writeJSON(err) {
        if (err) return console.log(err);
      });
      
      await producer_ubicaciones.connect()
      await producer_ubicaciones.send({
        topic: 'ubicaciones',
        messages: [
          { key: 'ubicaciones', value: JSON.stringify(aux), partition: 0 , },
        ],
      }
      )

      }
    }

    if(stock.length == 5){
      console.log("Entregas de un lote de 5 para reposicion de stock:")
      console.log(stock)
      stock.pop()
      stock.pop()
      stock.pop()
      stock.pop()
      stock.pop()
    }
  console.log("\n")
  res.send({
    mensaje: "Venta Registrada con Exito"
  })

})

// Calculo de ventas diarias (1 Dia  = 5 Min)
app.get('/ventasdiarias', async (req, res)=>{
  const fileName = './venta.json';
  const file = require(fileName);

  var datos = []
  var ruts = []

  for(i in file.registro){
    // Si ya lo guarde, le sumo los datos ya existentes
    if(ruts.includes(file.registro[i].RutMaestro.toString())){
      for(j in datos){
        if(datos[j].RutMaestro.toString() === file.registro[i].RutMaestro.toString()){
          datos[j] = {
            "RutMaestro" : datos[j].RutMaestro,
            "CantidadClientes": parseInt(datos[j].CantidadClientes) + 1,
            "CantidadSopaipillas": parseInt(datos[j].CantidadSopaipillas) + parseInt(file.registro[i].CantSopaipillas)
          }
        }
      }
    }
    // Si no esta lo inicializo
    else{
      ruts.push(file.registro[i].RutMaestro)
      datos.push(
        {"RutMaestro": file.registro[i].RutMaestro,
         "CantidadClientes": 1,
         "CantidadSopaipillas": file.registro[i].CantSopaipillas}
      )
    }

    }
    console.log("Imprime")
    console.log(datos)

})

app.get('/ventas', async (req, res) => {
  const fileName = './venta.json';
  const file = require(fileName);
  console.log(file.registro)
  res.send({
    mensaje: "Registro Ventas"
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

// Punto 3: ubicaciones y profugos
app.get('/vercarritos', async (req, res) => {

for(i in file3.registro){
  random = Math.floor(Math.random() * 10)
  random1 = Math.floor(Math.random() * 10)

  console.log(" - - - ")
  console.log(file3.registro[i].RutMaestro)
  console.log(parseInt(file3.registro[i].Ubicacion[0]) + random, ",", parseInt(file3.registro[i].Ubicacion[2]) + random1)
  console.log(" - - - ")
}

})


//


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
  console.log(`Cliente Corriendo`);
  consumidor();
});