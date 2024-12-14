import logging
import os
from flask import Flask, jsonify, request, redirect # type: ignore
from externalClusterManager.externalClusterManager import ExternalClusterManager as ex


app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Producer:

    def __init__(self):
        """
        Initializes the producer with the ZooKeeper connection details and the number of partitions.

        Args:
            ZOOKEEPER_HOST (str): Comma-separated list of ZooKeeper host:port strings.
            num_partitions (int): The number of partitions for the topic
        """
        self.externalClusterManager = ex.ExternalClusterManager()
        self.externalClusterManager.start()
        self.externalClusterManager.healthCheck()
        self.num_of_partitions= os.getenv("num_of_partitions") 

        logging.info("Producer initialized, connection to zookeeper started")

    
    def create_topic(self, topic_name):
        """
        Creates a topic using the externalClusterManager
        """
        if not self.externalClusterManager.topic_exists(topic_name):
            logging.info("Topic does not exist, attempting creation")
            self.externalClusterManager.create_topic(topic_name)
        else:
            logging.info(f"Topic with name: '{topic_name}' already exists, not creating it")
            
    
    def produce(self, topic_name, message):
        """
        Simulates writing a message to a topic, using the metadata stored in ZooKeeper to identify the correct partition

        Args:
            topic_name (str): The name of the topic to write to.
            message (str): The message to send.
        """
        logging.info(f"Producing message '{message}' to topic '{topic_name}'")


        try:
            self.externalClusterManager.create_topic(topic_name)
            logging.info(f"Message '{message}' sent to topic '{topic_name}'.")

        except:
            logging.error(f"An error occurred during message production: {e}")

    def get_topics(self):
        topics = self.externalClusterManager.getTopics()
        if not topics:
            return []
        return topics
    

@app.route("/produce/<topic_name>", methods=["POST"])
def handle_produce(topic_name):
    message = request.get_json().get("message")
    if not message:
        return jsonify({"error": "Missing message"}), 400
    
    if not topic_name:
        producer.create_topic(topic_name, message)

    producer.produce(topic_name, message)
    return jsonify({"message": "Message produced successfully"})

@app.route("/get-topics/", methods=["GET"])
def get_topics():
    topics = producer.get_topics()
    return jsonify(topics)

if __name__ == "__main__":
    try:
       producer = Producer() 
       app.run(host="0.0.0.0", port=os.getenv("PRODUCER_PORT"))
    except Exception as e:
        logging.error(f"Application failed to start {e}")