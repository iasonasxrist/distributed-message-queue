import logging
import os
from flask import Flask, jsonify, request  # type: ignore
from externalClusterManager.externalClusterManager import ExternalClusterManager as ex
from dotenv import load_dotenv

load_dotenv() # Load environment variables

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
ZOOKEEPER_HOST = os.environ.get("ZOOKEEPER_HOST", "zookeeper:2181")


class Producer:
    def __init__(self):
        """
        Initializes the producer with the ZooKeeper connection details and the number of partitions.
        """
    
        self.externalClusterManager = ex()
        self.num_of_partitions= os.getenv("num_of_partitions") 
        print(f"{self.num_of_partitions}*****************")
        logging.info("Producer initialized, connection to zookeeper started")

    def start(self):
        try:
            app.run(host="0.0.0.0", port=os.getenv("PRODUCER_PORT"))
            logging.info("Server is up and running")
        except Exception as e:
            logging.error(f"Application failed to start: {e}")

    def create_topic(self, topic_name)->bool:
        """
        Creates a topic using the externalClusterManager
        """
        if not self.externalClusterManager.topic_exists(topic_name):
            logging.info("Topic does not exist, attempting creation")
            success = self.externalClusterManager.create_topic(topic_name) # Store if the topic creation was succesful
            return success # Return the success value from cluster manager
        else:
            logging.info(f"Topic with name: '{topic_name}' already exists, not creating it")
            return True
            
    
    def produce(self, topic_name, message):
        """
        Simulates writing a message to a topic.

        Args:
            topic_name (str): The name of the topic to write to.
            message (str): The message to send.
        """
        logging.info(f"Producing message '{message}' to topic '{topic_name}'")
        try:
            # Do not create a topic every time a message is produced.
            logging.info(f"Message '{message}' sent to topic '{topic_name}'.")
            return True # Return True if successful.

        except Exception as e:
            logging.error(f"An error occurred during message production: {e}")
            return False # Return False if message production fails.

    def get_topics(self)-> list:
        topics = self.externalClusterManager.retrieve_topics()
        if not topics:
            return []
        return topics
    

@app.route("/produce/<topic_name>", methods=["POST"])
def handle_produce(topic_name):
    try:
        message = request.get_json().get("message")
        if not message or not topic_name:
            return jsonify({"error": "Missing payload"}), 400
        
        topics = producer.get_topics()
        if topic_name in topics:
            return jsonify({"error": "Topic already exists"}), 409 # Use 409 Conflict
        
        success = producer.create_topic(topic_name)
        if not success:
            return jsonify({"error": "Failed to create topic"}), 500
            
        success = producer.produce(topic_name, message) # Store if the message was produced successfully
        if not success:
           return jsonify({"error": "Failed to produce message"}), 500
        return jsonify({"message": "Message produced successfully"}), 201 # Use 201 Created
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred ", }), 500 # Return generic error and log it.
        

    
@app.route("/get-topics/", methods=["GET"])
def get_topics():
    topics = producer.get_topics()
    return jsonify(topics)

if __name__ == "__main__":
    producer = Producer()
    producer.start()