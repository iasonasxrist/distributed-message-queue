import logging
import json
import time
import kazoo
from kazoo.client import KazooClient, KazooState
import os
from collections import defaultdict
from dotenv import load_dotenv
from kazoo.exceptions import  ConnectionLoss

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
ZOOKEEPER_HOST = os.environ.get("ZOOKEEPER_HOST", "zookeeper:2181")
REGISTRATION_ZK_PATH="topics"

class ExternalClusterManager:
    """
    Manages topics in ZooKeeper, including creation and verification.
    """

    def __init__(self):
        """
        Initializes the topic manager with the ZooKeeper connection details.
        """
        self.num_partitions = int(os.getenv("num_partitions", "3"))  # Default to 3 partition if not specified
        self.connection = False
        self.registration_path = os.getenv("REGISTRATION_ZK_PATH", REGISTRATION_ZK_PATH) # Set registration path correctly
        self.zkClient= KazooClient(hosts=ZOOKEEPER_HOST)
        try:
            self.try_connect()
            self.healthCheck()
            self.ensure_root_path() # Ensure that the root path exists.
        except Exception as e:
            raise Exception(f"Zookeeper client failed to be started with error {e}")

    def ensure_root_path(self):
        """
        Ensure the root registration path exists
        """
        root_path = f"/{self.registration_path}"
        if not self.zkClient.exists(root_path):
          self.zkClient.ensure_path(root_path)
          logging.info(f"Root path created at {root_path}")
        else:
          logging.info(f"Root path found at {root_path}")
    def try_connect(self, max_retries=5, retry_delay=1):
        for i in range(max_retries):
            try:
                self.zkClient.start()
                break
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep((retry_delay + i))
                    continue
                else:
                    raise Exception("Could not connect to ZooKeeper after multiple attempts") from e

    def healthCheck(self):
        @self.zkClient.add_listener
        def checkConnectionStatus(status):
            if status == KazooState.LOST:
                logging.warning("Connection to ZooKeeper lost")
                self.connection = False
                self.try_connect()
            elif status == KazooState.SUSPENDED:
                logging.warning("Connection to ZooKeeper suspended")
                self.connection = False
            else:
                logging.info("Connected to Zookeeper")
                self.connection = True
    def stop(self):
        """
        Stops and closes the ZooKeeper client connection.
        """
        self.zkClient.stop()
        self.zkClient.close()
        logging.info("ExternalClusterManager connection to ZooKeeper closed")

    def create_topic(self, topic_name):
        """
        Creates a topic structure in ZooKeeper, including metadata and partitions.

        Args:
            topic_name (str): The name of the topic to create.
        """
        num_partitions= int(os.getenv("num_partitions", 3))

        if num_partitions is None:
            num_partitions = self.num_partitions

        logging.info(f"Creating topic '{topic_name}' with {num_partitions} partitions.")

        try:
            topic_path = f"/{os.getenv('REGISTRATION_ZK_PATH', REGISTRATION_ZK_PATH)}/{topic_name}"
            self.zkClient.ensure_path(os.path.dirname(topic_path))
            logging.info(f"Created or ensured topic path '{topic_path}'")

            # Create Metadata
            metadata_path = f"{topic_path}/metadata"
            metadata_data = {"partitions": num_partitions}
            self.zkClient.create(metadata_path, value=json.dumps(metadata_data).encode(), makepath=True)
            logging.info(f"Created metadata path '{metadata_path}' with data: {metadata_data}")

            self.assign_brokers_to_partitions(topic_name)
            logging.info(f"Successfully created topic '{topic_name}'")
            return True

        except Exception as e:
            logging.error(f"An error occurred creating topic '{topic_name}': {e}")
            return False

    def get_available_brokers_per_partition_per_topic(self, topic_name):
        """
        Retrieves a dictionary of brokers for each partition of a given topic.

        Returns:
            dict: A dictionary where keys are partition IDs and values are
                  lists of broker IDs assigned to those partitions.
        """
        brokers_per_partition = defaultdict(list)
        try:
            topic_path = f"/{os.getenv('REGISTRATION_ZK_PATH', REGISTRATION_ZK_PATH)}/{topic_name}"
            if not self.zkClient.exists(topic_path):
                logging.warning(f"Topic path '{topic_path}' does not exist.")
                return {}

            for i in range(self.num_partitions):
                partition_id = f"partition{i}"
                partition_path = f"{topic_path}/{partition_id}"
                broker_path = f"{partition_path}/broker"
                if self.zkClient.exists(broker_path):
                  data, _ = self.zkClient.get(broker_path)
                  broker_id = data.decode()
                  brokers_per_partition[partition_id].append(broker_id)
                  logging.debug(f"Found broker '{broker_id}' for partition '{partition_path}'")
                else:
                  logging.warning(f"Broker not found at '{broker_path}'")
            return brokers_per_partition

        except Exception as e:
           logging.error(f"Error while retrieving brokers per partition: {e}")
           return {}
        
    def assign_brokers_to_partitions(self, topic_name):
        """
        Assigns brokers to the topic partitions.
        """
        brokers_per_partition = self.get_available_brokers_per_partition_per_topic(topic_name)
        if not brokers_per_partition:
            logging.warning("No brokers available for the partition assignments")
            return
        
        for partition_id, available_brokers in brokers_per_partition.items():
           partition_path = f"/{os.getenv('REGISTRATION_ZK_PATH', REGISTRATION_ZK_PATH)}/{topic_name}/{partition_id}"
           self.zkClient.ensure_path(partition_path)
           if available_brokers:
              # Assign one broker to this partition
              # we can implement any assignment logic here, for example choosing the first broker available.
              assigned_broker = available_brokers[0]
              broker_path = f"{partition_path}/broker"
              self.zkClient.create(broker_path, value=assigned_broker.encode(), makepath=True)
              logging.info(f"Assigned broker '{assigned_broker}' to partition {partition_id}")
           else:
              logging.warning(f"No brokers available for partition: '{partition_id}' of topic '{topic_name}'")
          
    def topic_exists(self, topic_name):
        """
        Check if the topic exists in ZooKeeper.

        Args:
            topic_name (str): Name of the topic
        Returns:
            (bool): True if exists, False otherwise
        """
        topic_path = f"/{self.registration_path}/{topic_name}"
        logging.debug(f"Checking if topic exists at {topic_path}") # Debug log added
        try:
            return self.zkClient.exists(topic_path) is not None
        except ConnectionLoss as e:
            logging.error(f"Zookeeper connection loss on topic_exists: {e}")
            self.try_connect()
            return False
        except Exception as e:
            logging.error(f"An error occurred on topic_exists: {e}")
            return False

    def retrieve_topics(self):
      """
      Retrieves the list of available topics registered in zookeeper.
      """
      topic_path = f"/{os.getenv('REGISTRATION_ZK_PATH', REGISTRATION_ZK_PATH)}/"
      logging.debug(f"Retrieving topics from path: {topic_path}")
      try:
          topics = self.zkClient.get_children(topic_path)
          logging.debug(f"Retrieved topics: {topics}")
          return sorted(topics)
      except ConnectionLoss as e:
          logging.error(f"Zookeeper connection loss during topic retrieval {e}")
          self.try_connect()
          return []
      except Exception as e:
          logging.error(f"An error occured during topic retrieval: {e}")
          return []