import logging
import os
from flask import Flask, jsonify, request  # type: ignore
from externalClusterManager.externalClusterManager import ExternalClusterManager as ex
from collections import defaultdict
from dotenv import load_dotenv


app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

zookeeper_host = os.environ.get("ZOOKEEPER_HOST", "zookeeper:2181")
hostname = os.environ.get("HOSTNAME")
class Broker:
    """
    Manages message storage, partition assignment and the connection to zookeeper.
    """

    def __init__(self):
        """
        Initializes the broker with a unique id and sets up connection to zookeeper.
        """
        self.broker_id = None
        self.registration_path = os.getenv("REGISTRATION_ZK_PATH")
        self.local_storage = defaultdict(list) #key: partition_id, value: list of messages.
        self.externalClusterManager = ex()
        self.connection = True
        logging.info(f"***Broker initializing***")

    def start(self):
        """
        Establishes connection to zookeeper
        """
        try:
            self.register_broker()
        except Exception as e:
             logging.error(f"Error connecting to zookeeper: {e}")
             raise

    def register_broker(self):
        """
        Registers the broker in ZooKeeper, creating the `/brokers/<broker_id>` path.
        """
        broker_path = f"/brokers"
        try:
            registered_path = self.externalClusterManager.registerNode(broker_path, hostname)
            if registered_path:
              self.broker_id = registered_path.split("/")[-1]
        except Exception as e:
            logging.error(f"Failed to register broker: {e}")
            raise e

    def stop(self):
        """
        Unregisters the broker from ZooKeeper and closes connection
        """
        try:
            if self.connection:
              self.externalClusterManager.close()
              self.connection = False
              logging.info("Broker connection to zookeeper closed")
        except Exception as e:
            logging.error(f"Error while unregistering broker: {e}")

    def has_partition(self, topic_name, partition_id):
        """
        Checks if the broker has any messages for a given partition.
        """
        try:
           if not self.is_responsible_for_partition(topic_name, partition_id):
                return {"error": f"This broker is not responsible for topic '{topic_name}' and partition '{partition_id}'"}
           
           if partition_id in self.local_storage:
                return {"data": self.local_storage[partition_id]}
           else:
                return {"data": f"No messages for partition '{partition_id}' in topic '{topic_name}'"}
        except Exception as e:
            logging.error(f"Error while checking the messages on the local storage: {e}")
            return {"error": f"Error: {e}"}

    def is_responsible_for_partition(self, topic_name, partition_id):
        """
        Check if this broker is the one registered to store messages for the topic and partition, using the data from Zookeeper.
        """
        try:
            partition_path = f"/{self.registration_path}/{topic_name}/{partition_id}"
            if self.externalClusterManager.exists(partition_path):
                children = self.externalClusterManager.get_children(partition_path)
                for child in children:
                    if child.startswith("broker"):
                        broker_path = f"{partition_path}/{child}"
                        data, _ = self.externalClusterManager.get(broker_path)
                        broker_id = data.decode()
                        return broker_id == self.broker_id
            return False
        except Exception as e:
           logging.error(f"Error checking if broker is responsible for topic: {e}")
           return False

    def store_message(self, topic_name, partition_id, message):
        """
        Stores the message for the specific topic and partition.
        """
        try:
            if not self.is_responsible_for_partition(topic_name, partition_id):
                raise Exception("Broker is not responsible for the specified topic and partition")
            self.local_storage[partition_id].append(message)
            logging.info(f"Broker {self.broker_id} stored message for partition '{partition_id}'")

        except Exception as e:
            logging.error(f"Error while storing message: {e}")

    def get_messages(self, topic_name, partition_id):
        """
        Retrieves messages for a given topic and partition id.
        """
        try:
            if not self.is_responsible_for_partition(topic_name, partition_id):
                raise Exception("Broker is not responsible for the specified topic and partition")
            return self.local_storage[partition_id]
        except Exception as e:
            logging.error(f"Error while fetching messages from local storage: {e}")
            return []

broker = None
@app.route("/<topic_name>/has_partition/<partition_id>", methods=["GET"])
def handle_has_partition(topic_name, partition_id):
    try:
      result = broker.has_partition(topic_name, partition_id)
      return jsonify(result)
    except Exception as e:
      return jsonify({"error": f"Internal Server Error: {e}"}), 500

@app.route("/<topic_name>/store/<partition_id>", methods=["POST"])
def handle_store_message(topic_name, partition_id):
    try:
      message = request.get_json().get("message")
      if not message:
        return jsonify({"error": "Missing message"}), 400
      broker.store_message(topic_name, partition_id, message)
      return jsonify({"message": "Message produced successfully"})
    except Exception as e:
      return jsonify({"error": f"Error while storing message: {e}"}), 500
    
@app.route("/<topic_name>/get_messages/<partition_id>", methods=["GET"])
def handle_get_messages(topic_name, partition_id):
    try:
      messages = broker.get_messages(topic_name, partition_id)
      return jsonify({"messages": messages})
    except Exception as e:
      return jsonify({"error": f"Error while retrieving messages: {e}"}), 500

if __name__ == "__main__":
    try:
        broker = Broker()
        broker.start()
        app.run(host="0.0.0.0", port=os.getenv("BROKER_PORT"))
    except Exception as e:
        logging.error(f"Application failed to start {e}")