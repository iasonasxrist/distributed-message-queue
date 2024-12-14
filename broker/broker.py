import logging
import os
from flask import Flask, jsonify, request, redirect # type: ignore
from externalClusterManager.externalClusterManager import ExternalClusterManager as ex
import uuid

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Broker:

    def __init__(self, broker_id=None):
        """
        Initializes the broker with the ZooKeeper connection details and the number of partitions.

        Args:
            ZOOKEEPER_HOST (str): Comma-separated list of ZooKeeper host:port strings.
            num_partitions (int): The number of partitions for the topic
        """
        self.externalClusterManager = ex.ExternalClusterManager()
        self.broker_id= broker_id if broker_id else str(uuid.uuid4())
        self.local_storage={} #key:partition_id
        self.connection = False
        logging.info("***Broker initialized ID: {self.broker_id}***")

    def has_partition(self, partition_id):
        if partition_id not in self.local_storage:
            return f"there is no partion store in broker: {self.broker_id}"
        else:
            return {"data" :self.local_storage[partition_id]}
        

if __name__ == "__main__":
    try:
       broker = Broker(1) 
       app.run(host="0.0.0.0", port=os.getenv("BROKER_PORT"))
    except Exception as e:
        logging.error(f"Application failed to start {e}")