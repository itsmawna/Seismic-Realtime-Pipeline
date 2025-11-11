const express = require("express");
const { Kafka } = require("kafkajs");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

const kafka = new Kafka({
  clientId: "seismic-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "seismic-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "RawSeismicData", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      io.emit("newEvent", data);  // envoie au frontend
    },
  });
};

run().catch(console.error);

server.listen(4000, () => console.log("Backend running on http://localhost:4000"));
