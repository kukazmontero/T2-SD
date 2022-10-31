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
const producer_coordenadas = kafka.producer();

const consumidor_usuarios = kafka.consumer({ groupId: 'grupo1' })
const consumidor_ventas = kafka.consumer({groupId: 'grupo2'})
const consumidor_ubicaciones = kafka.consumer({ groupId: 'grupo3' })

var stock = [];


app.get("/listapremium", async (req, res) => {
  // Producing
  let jsonData = require('./users.json');
  for(i in jsonData.premium){
      jsonData.premium[i].numerolista = i;
      await producer.connect()
      await producer.send({
        topic: 'usuarios',
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
        topic: 'usuarios',
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
    //console.log(file.premium)
  }
  if(nuevousuario.Premium === "No"){
    file.nopremium.push(nuevousuario)
    //console.log(file.nopremium)
  }
  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    //console.log(JSON.stringify(file));
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
      file.premium[numero].Estado = "Normal"
      file.miembros.push(file.premium[numero])
      var deletefile = file.premium.splice(numero, 1)
      res.send({
        mensaje: "Usuario aceptado"
      })
    }
    if(aceptado === "No") {
      //console.log(file.premium[numero]) 
      var deletefile = file.premium.splice(numero, 1)
      //console.log(file.premium)
      res.send({
        mensaje: "Usuario No aceptado"
      })
    }
    /*delete file.premium[numero]
    console.log(file.premium)*/
  }
  if(premium === "No") {
    if(aceptado === "Si") {
      //console.log("Si")
      file.nopremium[numero].estado = "Normal";
      file.miembros.push(file.nopremium[numero])
      var deletefile = file.nopremium.splice(numero, 1)
      res.send({
        mensaje: "Usuario aceptado"
      })
    }
    if(aceptado === "No") {
      //console.log("No")
      //console.log(file.nopremium[numero]) 
      var deletefile = file.nopremium.splice(numero, 1)
      //console.log(file.nopremium)
      res.send({
        mensaje: "Usuario No aceptado"
      })
    }
    
  }

  fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
    if (err) return console.log(err);
    //console.log(JSON.stringify(file));
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

  
  await producer_ventas.connect()
  await producer_ventas.send({
  topic: 'ventas',
  messages: [
        { key: 'ventas', value: JSON.stringify(nuevaventa), partition: 0,},
      ]
    }) 

  console.log("Registro de venta Ingresado")
  //console.log(JSON.stringify(nuevaventa))
  
  // Si una venta contiene un stock <= 20, se guarda en un arreglo 
  if(parseInt(nuevaventa.StockRestante) <= 20){
    stock.push(nuevaventa)
    console.log("Poco stock")
    console.log("\n")
    }

    if(stock.length == 5){
      console.log("Entregas de un lote de 5 para reposicion de stock:")
      //console.log(stock)
      await producer_ventas.connect()
      await producer_ventas.send({
      topic: 'ventas',
      messages: [
          { key: 'reposicion', value: JSON.stringify(stock), partition: 1},
        ],
      }) 

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
    console.log("Resultados Diarios:")
    for(i in datos){
      console.log("Rut Maestro: ", datos[i].RutMaestro)
      console.log("Cantidad de Clientes Totales: ", datos[i].CantidadClientes)
      console.log("Cantidad de Sopaipillas Totales: ", datos[i].CantidadSopaipillas)
      console.log("Promedio de Ventas: ", parseInt(datos[i].CantidadSopaipillas) / parseInt(datos[i].CantidadClientes), "Sopaipillas")
      console.log(" - - - ")
    }
    res.send({
      mensaje: "Reistro de Ventas"
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

app.get('/miembros', async (req, res) => {
  const fileName = './users.json';
  const file = require(fileName);
  console.log(file.miembros)
  res.send({
    mensaje: "Lista miembros"
  })
})


app.post('/reportar', async (req, res) => {
  const fileName = './users.json';
  const file = require(fileName);
  let patente = req.query.Patente;
  let coordenadas = req.query.Coordenadas;
  //console.log(patente)
  for(i in file.miembros){
    if(patente === file.miembros[i].Patente){
      file.miembros[i].Estado = "Profugo"
      fs.writeFile(fileName, JSON.stringify(file), function writeJSON(err) {
        if (err) return console.log(err);
        });
      var aux = {
        "patente" : patente,
        "coordenadas" : coordenadas
      }
      //console.log(aux)
      await producer_coordenadas.connect()
      await producer_coordenadas.send({
        topic: 'Coordenadas',
        messages: [
          { key: 'Profugo', value: JSON.stringify( aux), partition: 1, },
        ]
      })
    }
    
  }
  await producer_coordenadas.disconnect()
  res.send({
    message: 'Reporte enviado'
  })

})

// - - . 


async function coordenadas() {
  const fileName = './users.json';
  const file = require(fileName)

  for(i in file.miembros){
    if(file.miembros[i].Estado === "Normal"){
    random = Math.floor(Math.random() * 10)
    random1 = Math.floor(Math.random() * 10)

    var coordenadas = {
      "patente" : file.miembros[i].Patente,
      "coordenadas" : random + ", " + random1
    }

    await producer_coordenadas.connect()
      await producer_coordenadas.send({
        topic: 'Coordenadas',
        messages: [
          { key: 'Coordenadas', value: JSON.stringify( coordenadas), partition: 0, },
        ]
      })
  }}
  await producer_coordenadas.disconnect()
}


async function consumidor() {
  consumidor_usuarios.connect()
  await consumidor_usuarios.subscribe({ topic: 'usuarios', fromBeginning: true })
  await consumidor_usuarios.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      if(partition ===0){
        console.log({
            Lista: 'Premium',
            value:JSON.parse(message.value.toString()),
            partition,
        })
      }
      else if(partition ===1){
        console.log({
            Lista: 'No Premium',
            value:JSON.parse(message.value.toString()),
            partition,
        })
      }
    },
})

consumidor_ventas.connect()
await consumidor_ventas.subscribe({ topic: 'ventas', fromBeginning: true })
await consumidor_ventas.run({
  eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
    if(partition ===0){
      console.log({
          Venta: 'General',
          value:JSON.parse(message.value.toString()),
          partition,
      })
    }
    else if(partition ===1){
      console.log({
          Venta: 'Reposicion',
          value:JSON.parse(message.value.toString()),
          partition,
      })
    }
  },
})

consumidor_ubicaciones.connect()
await consumidor_ubicaciones.subscribe({ topic: 'Coordenadas', fromBeginning: true })
await consumidor_ubicaciones.run({
  partitionsConsumedConcurrently: 2,
  eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
    //console.log("Holaaaaaaaaa")
    if(partition ===0){
      console.log({
          value:JSON.parse(message.value.toString()),
          partition,
      })
    }
    if(partition ===1){
      console.log({
          title: 'El carrito esta profugo',
          value:JSON.parse(message.value.toString()),
          partition,
      })
    }
  },
})

};


app.listen(port, () => {
  console.log(`Cliente Corriendo`);
  consumidor();
  setInterval(coordenadas, 30000);
});