import logging
import os
from flask import Flask, jsonify, request, redirect
# from external_cluster_manager.externalClusterManager import ExternalClusterManager
from external_cluster_manager.externalClusterManager import ExternalClusterManager
app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Producer:

    def __init__(self):
        """
        Initializes the producer with the ZooKeeper connection details and the number of partitions.

        Args:
            zk_hosts (str): Comma-separated list of ZooKeeper host:port strings.
            num_partitions (int): The number of partitions for the topic
        """
        self.externalClusterManager = ExternalClusterManager()
        self.num_of_partitions= os.getenv("num_of_partitions") 
    
        logging.info("Producer initialized, connection to zookeeper started")
    
    def produce(self, topic_name, message):
        """
        Simulates writing a message to a topic, using the metadata stored in ZooKeeper to identify the correct partition

        Args:
            topic_name (str): The name of the topic to write to.
            message (str): The message to send.
        """
        logging.info(f"Producing message '{message}' to topic '{topic_name}'")


        try:
            pass

        except:

            pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.getenv("PORT"))