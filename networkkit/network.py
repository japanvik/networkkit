import asyncio
from typing import Any, Protocol

import aiohttp
import requests
import zmq

from networkkit.messages import Message


class Subscriber(Protocol):
    """
    Interface defining a subscriber for receiving messages.

    This is a protocol class outlining the expected behavior of message subscribers.
    Concrete implementations must provide methods for handling messages and determining if a message is intended for them.
    """

    name: str

    async def handle_message(self, message: Message) -> Any:
        """
        Asynchronous method for handling received messages.

        This method is called whenever a message is received that is intended for this subscriber.
        The specific implementation of handling the message is left to the concrete subscriber class.

        Args:
            message: The received message object (type: networkkit.messages.Message)

        Returns:
            Any: The return value from the subscriber's message handling logic (implementation dependent)
        """

        raise NotImplementedError

    def is_intended_for_me(self, message: Message) -> bool:
        """
        Method to determine if a message is intended for this subscriber.

        This method is used by the ZMQMessageReceiver to filter messages and deliver them to the appropriate subscribers.

        Args:
            message: The received message object (type: networkkit.messages.Message)

        Returns:
            bool: True if the message is intended for this subscriber, False otherwise (implementation dependent)
        """

        raise NotImplementedError


class MessageSender(Protocol):
    """
    Interface defining a message sender.

    This is a protocol class outlining the expected behavior of message senders. 
    Concrete implementations must provide a method for sending messages over the network.
    """

    def send_message(self, message: Message) -> Any:
        """
        Method to send a message over the network.

        This method is responsible for sending the provided message through the chosen network protocol.

        Args:
            message: The message object to be sent (type: networkkit.messages.Message)

        Returns:
            Any: The return value from the message sending operation (implementation dependent)
        """

        raise NotImplementedError


import asyncio
import logging
from typing import Any, List, Optional

import zmq
import zmq.asyncio

from networkkit.messages import Message
from networkkit.network import Subscriber


class ZMQMessageReceiver:
    """
    Class to receive messages using ZeroMQ and distribute them to registered subscribers.

    This class establishes a ZeroMQ subscriber socket and listens for messages on a specified address.
    It then processes received messages, validates them using the Message model, and distributes them to registered subscribers
    based on their `is_intended_for_me` method.
    """

    def __init__(self, subscribe_address: str = "tcp://127.0.0.1:5555"):
        """
        Constructor for the ZMQMessageReceiver class.

        Args:
            subscribe_address (str, optional): The ZeroMQ subscriber socket address. Defaults to "tcp://127.0.0.1:5555".
        """
        self.subscribe_address = subscribe_address
        self.subscribers: List[Subscriber] = []

        # Initialize zmq.asyncio context and socket
        self.context = zmq.asyncio.Context()
        self.pubsub_subscriber = self.context.socket(zmq.SUB)
        self.pubsub_subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        self.pubsub_subscriber.connect(self.subscribe_address)
        self.running = False

    async def start(self):
        """
        Asynchronous method to start receiving messages from the ZeroMQ subscriber socket.

        This method continuously receives messages, validates them, and distributes them to subscribers.
        It remains running until stopped by the `stop` method.
        """
        self.running = True
        logging.info(f"ZMQMessageReceiver connected to {self.subscribe_address}")
        try:
            while self.running:
                try:
                    raw_message = await self.pubsub_subscriber.recv_json(flags=zmq.NOBLOCK)
                    message = Message.model_validate(raw_message)
                    await self.handle_message(message)
                except zmq.Again:
                    # No message received; sleep briefly to prevent busy waiting
                    await asyncio.sleep(0.1)
                except zmq.ZMQError as e:
                    if e.errno == zmq.ETERM:
                        # Context terminated
                        logging.info("ZMQ context terminated.")
                        break
                    else:
                        logging.error(f"ZMQ Error: {e}")
                        break
                except asyncio.CancelledError:
                    # Handle task cancellation gracefully
                    logging.info("ZMQMessageReceiver task cancelled.")
                    break
                except Exception as e:
                    logging.error(f"Unexpected error in ZMQMessageReceiver: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """
        Asynchronous method to stop receiving messages and clean up resources.

        This method sets the running flag to False, closes the ZeroMQ socket, and terminates the context.
        """
        if self.running:
            self.running = False
            logging.info("Stopping ZMQMessageReceiver...")
            try:
                self.pubsub_subscriber.close(linger=0)
                self.context.term()
                logging.info("ZMQMessageReceiver stopped successfully.")
            except Exception as e:
                logging.error(f"Error during ZMQMessageReceiver shutdown: {e}")

    def register_subscriber(self, subscriber: Subscriber):
        """
        Method to register a subscriber with the ZMQMessageReceiver.

        This method adds the provided subscriber object to the list of subscribers.
        Messages will then be distributed to this subscriber based on its `is_intended_for_me` method.

        Args:
            subscriber (Subscriber): The subscriber object to be registered.
        """
        self.subscribers.append(subscriber)
        logging.info(f"Subscriber {subscriber.name} registered.")

    async def handle_message(self, message: Message):
        """
        Asynchronous method to distribute a message to registered subscribers.

        This method iterates through the list of registered subscribers and calls their `handle_message` method
        if the `is_intended_for_me` method of the subscriber returns True for the received message.

        Args:
            message: The received message object (type: networkkit.messages.Message)
        """
        for subscriber in self.subscribers:
            if subscriber.is_intended_for_me(message):
                await subscriber.handle_message(message)


class HTTPMessageSender:
    """
    Class to send messages over HTTP using requests.

    This class implements the MessageSender protocol and provides a method to send messages as JSON payloads to a specified HTTP endpoint.
    """

    def __init__(self, publish_address: str = "http://127.0.0.1:8000") -> None:
        """
        Constructor for the HTTPMessageSender class.

        Args:
            publish_address (str, optional): The HTTP endpoint URL for message publishing. Defaults to "http://127.0.0.1:8000".
        """

        self.publish_address = publish_address
        self.session = aiohttp.ClientSession()

    async def send_message(self, message: Message) -> str:
        """
        Method to send a message as a JSON payload to the configured HTTP endpoint.

        This method utilizes the `requests` library to send a POST request to the publish address with the message data converted to JSON format.

        Args:
            message: The message object to be sent (type: networkkit.messages.Message)

        Returns:
            requests.Response: The response object from the HTTP POST request.
        """
        try:
            async with self.session.post(f"{self.publish_address}/data", json=message.model_dump()) as response:
                if response.status != 200:
                    logging.error(f"Failed to send message to {self.publish_address}: {response.status}")
                return await response.text()
        except Exception as e:
            logging.error(f"Exception during sending message: {e}")
            raise e

    async def close(self):
        await self.session.close()
