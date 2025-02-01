import logging
import os
import time
from flask import Flask, jsonify, request  # type: ignore
from externalClusterManager.externalClusterManager import ExternalClusterManager as ex
from dotenv import load_dotenv
import requests

load_dotenv()  # Load environment variables

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
ZOOKEEPER_HOST = os.environ.get("ZOOKEEPER_HOST", "zookeeper:2181")


class Producer:
    def __init__(self):
        """
        Initializes the producer with the ZooKeeper connection details and the number of partitions.
        """
        self.externalClusterManager = ex()
        self.num_partitions = int(os.getenv("num_partitions", "3"))  # Default to 3 partitions if not specified
        self.brokers = []
        self.broker_assignment_tracker = {} # Dictionary to track leader assignments for partitions
        logging.info("Producer initialized, connection to zookeeper started")
        self.watch_topics()
        self.watch_brokers()

    def start(self):
        try:
            app.run(host="0.0.0.0", port=int(os.getenv("PRODUCER_PORT", "5001")))
            logging.info("Server is up and running")
        except Exception as e:
            logging.error(f"Application failed to start: {e}")

    def watch_topics(self):
        """
        Watch for new topics created under the /topics path.
        """
        @self.externalClusterManager.zkClient.ChildrenWatch("/topics")
        def on_topic_change(topics):
            if topics:
                for topic in topics:
                    topic_path = f"/topics/{topic}"
                    self.watch_partitions(topic_path)

    def watch_brokers(self):
        """
        Watches for changes in the `/brokers` path in ZooKeeper to keep track of available brokers.
        """
        @self.externalClusterManager.zkClient.ChildrenWatch("/brokers")
        def on_broker_change(brokers):
            """
            Args:
                brokers (list): A list of broker IDs.
            """
            if brokers:
                logging.info(f"| Detected changes | in brokers: {brokers}")
                self.brokers = brokers
                self.broker_assignment_tracker = {} # clear the current assignment in case there is a change in the brokers

    def watch_partitions(self, topic_path):
        """
        Watch for new partitions under a specific topic path.
        """
        @self.externalClusterManager.zkClient.ChildrenWatch(topic_path)
        def on_partition_change(children_names):
            for _, children in enumerate(children_names):
                if children.startswith("partition"):
                    partition_path = f"{topic_path}/{children}"
                    self.register_to_partition(partition_path)

    def register_to_partition(self, partition_path):
            """
            Register the broker under a specific partition path.
            Use a different leader broker for each partition.
            """
            try:
                if not self.brokers:
                    logging.error("No brokers available to assign as leader.")
                    return

                # Extract topic and partition name
                topic_name = partition_path.split("/")[2]
                partition_name = partition_path.split("/")[-1] # e.g., "partition-0"

                # Define key for broker assignment tracker.
                assignment_key = f"{topic_name}-{partition_name}"

                # If there is no assigned leader for this partition, then assign one.
                if assignment_key not in self.broker_assignment_tracker:
                    leader_broker_index = len(self.broker_assignment_tracker) % len(self.brokers) # Select a leader broker based on the amount of assigned partitions so far
                    leader_broker = self.brokers[leader_broker_index]
                    self.broker_assignment_tracker[assignment_key] = leader_broker
                else:
                    # If there is an assignment for this parition, get the broker from the dictionary.
                    leader_broker = self.broker_assignment_tracker[assignment_key]

                self.externalClusterManager.zkClient.create(
                    f"{partition_path}/",
                    value=bytes(leader_broker, 'utf-8'),
                    ephemeral=True
                )
                logging.info(f"Assigned broker {leader_broker} as leader for partition {partition_path}")

                replica_brokers = [broker for broker in self.brokers if broker != leader_broker]
                for replica_broker in replica_brokers:
                    # Added a placeholder message, because I cannot extract the correct message here.
                    self.send_message_to_broker(replica_broker, topic_name, "replica_message")
                    logging.info(f"Assigned broker {replica_broker} as replica for partition {partition_path}")

            except Exception as e:
                logging.error(f"Error on register to partition, {e}")
                raise e

    def create_topic(self, topic_name) -> bool:
        """
        Creates a topic using the externalClusterManager
        """
        if not self.externalClusterManager.topic_exists(topic_name):
            logging.info("Topic does not exist, attempting creation")
            success = self.externalClusterManager.create_topic(topic_name)  # Store if the topic creation was successful
            return success  # Return the success value from cluster manager
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
            # added broker_id and topic_name because it was missing. Also added the missing parameter to the method call.
            for broker in self.brokers:
                self.send_message_to_broker(broker, topic_name, message)
            logging.info(f"Message '{message}' sent to topic '{topic_name}'.")
            return True  # Return True if successful.

        except Exception as e:
            logging.error(f"An error occurred during message production: {e}")
            return False  # Return False if message production fails.

    def send_message_to_broker(self, broker_id, topic_name, message, retries=3):
        """
        Sends a message to a broker via HTTP with retry logic.
        """
        broker_url = f"http://{broker_id}:5000/receive_message"
        payload = {
            "topic": topic_name,
            "message": message
        }
        # Fixed how the broker requests are called.
        for attempt in range(retries):
            try:
                logging.info(f"Attempt {attempt + 1}: Sending message to broker {broker_id} at {broker_url}")
                response = requests.post(broker_url, json=payload, timeout=5)  # changed request with requests
                if response.status_code == 200:
                    logging.info(f"Message successfully sent to broker {broker_id}")
                    return
                else:
                    raise Exception(f"Broker returned status code {response.status_code}")
            except Exception as e:
                logging.error(f"Failed to send message to broker {broker_id}: {e}")
                time.sleep(1)

    def get_topics(self) -> list:
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
            return jsonify({"error": "Topic already exists"}), 409  # Use 409 Conflict

        success = producer.create_topic(topic_name)
        if not success:
            return jsonify({"error": "Failed to create topic"}), 500

        success = producer.produce(topic_name, message)  # Store if the message was produced successfully
        if not success:
            return jsonify({"error": "Failed to produce message"}), 500
        return jsonify({"message": "Message produced successfully"}), 201  # Use 201 Created

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred "}), 500  # Return generic error and log it.


@app.route("/get-topics/", methods=["GET"])
def get_topics():
    topics = producer.get_topics()
    return jsonify(topics)


if __name__ == "__main__":
    producer = Producer()
    producer.start()