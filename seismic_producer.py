from __future__ import unicode_literals
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen
from kafka import KafkaProducer
import logging
import json
import sys

# URL du flux sismique
echo_uri = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 15

# Connexion Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
TOPIC = "RawSeismicData"

# Fonction qui traite chaque message reçu du flux
def myprocessing(message):
    try:
        data = json.loads(message)
        info = data['data']['properties']
        info['action'] = data['action']

        # Affiche quelques infos
        logging.info(f"New event: Mag {info.get('mag')} - Region: {info.get('flynn_region')}")

        # Envoi du JSON dans Kafka
        producer.send(TOPIC, json.dumps(info).encode('utf-8'))
        producer.flush()

    except Exception:
        logging.exception("Error parsing or sending message")

@gen.coroutine
def listen(ws):
    while True:
        msg = yield ws.read_message()
        if msg is None:
            logging.info("Connection closed")
            break
        myprocessing(msg)

@gen.coroutine
def launch_client():
    try:
        logging.info(f"Connecting to {echo_uri}...")
        ws = yield websocket_connect(echo_uri, ping_interval=PING_INTERVAL)
    except Exception:
        logging.exception("Connection error")
    else:
        logging.info("✅ Listening for seismic events...")
        listen(ws)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    ioloop = IOLoop.instance()
    launch_client()
    try:
        ioloop.start()
    except KeyboardInterrupt:
        logging.info("Stopping WebSocket...")
        ioloop.stop()
