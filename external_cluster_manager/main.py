import logging
import json
import time
from kazoo.retry import KazooRetry
from kazoo.client import KazooClient, KazooState
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class ExternalClusterManager:
    """
    Manages topics in ZooKeeper, including creation and verification.
    """
    
    def __init__(self):
        """
        Initializes the topic manager with the ZooKeeper connection details.
        Args:
            zkClient_hosts (str): Comma-separated list of ZooKeeper host:port strings.
        """
        self.zkClient = os.getenv("zk_host", "localhost:2181")
        self.num_partitions = int(os.getenv("num_partitions", "3"))  # Default to 1 partition if not specified
        self.connection=False
        print(type(self.num_partitions ))

        try:
        
            self.zkClient.try_connect()
            self.zkClient.healthCheck()
        except:
            raise Exception("Zookeeper client failed to be started")


    def start(self):
        if not self.zkClient:
            logging.error("Check connection string connection to ExternalClusterManager")

        self.kz_retry = KazooRetry(max_tries=1000, delay=0.5, backoff=2)
        self.zkClient= KazooClient(hosts=self.zkClient, connection_retry=self.kz_retry, command_retry=self.kz_retry)
        
        logging.info("ExternalClusterManager initialized, connection to ZooKeeper started")

        
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
            num_partitions (int): The number of partitions for the topic.
        """
        num_partitions= os.getenv("num_partitions", 3)

        if num_partitions is None:
            num_partitions = self.num_partitions  # Use the default number of partitions from the environment

        logging.info(f"Creating topic '{topic_name}' with {num_partitions} partitions.")

        try:
            topic_path = f"/topics/{topic_name}"
            self.zkClient.ensure_path(topic_path)
            logging.info(f"Created or ensured topic path '{topic_path}'")

            # Create Metadata
            metadata_path = f"{topic_path}/metadata"
            metadata_data = {"partitions": num_partitions}
            self.zkClient.create(metadata_path, value=json.dumps(metadata_data).encode(), makepath=True)
            logging.info(f"Created metadata path '{metadata_path}' with data: {metadata_data}")

            # Create Partitions and Broker
            for i in range(num_partitions):
                partition_path = f"{topic_path}/partition{i}"
                self.zkClient.ensure_path(partition_path)
                broker_path = f"{partition_path}/broker"
                self.zkClient.create(broker_path, value=str(i + 1).encode(), makepath=True)  # Use a different broker ID for each partition
                logging.info(f"Created broker '{broker_path}' for partition {i}")

            logging.info(f"Successfully created topic '{topic_name}'")

        except Exception as e:
            logging.error(f"An error occurred creating topic '{topic_name}': {e}")

    def topic_exists(self, topic_name):
        """
        Check if the topic exists in ZooKeeper.

        Args:
            topic_name (str): Name of the topic
        Returns:
            (bool): True if exists, False otherwise
        """
        topic_path = f"/topics/{topic_name}"
        return self.zkClient.exists(topic_path) is not None


    topic_name = "example-topic-2"
    topic_manager.create_topic(topic_name=topic_name)
    exists = topic_manager.topic_exists(topic_name="example-topic")
    logging.info(f"Topic 'example-topic' exists: {exists}")
    exists = topic_manager.topic_exists(topic_name=topic_name)
    logging.info(f"Topic '{topic_name}' exists: {exists}")
