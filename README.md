# NetworkKit

NetworkKit is a simple yet powerful communication framework designed to streamline the interaction between distributed agents. Originally built for AI agent swarms, NetworkKit leverages both publish/subscribe (pub/sub) and HTTP protocols to enable seamless and scalable message exchange across diverse systems. Whether you are building a network of IoT devices, a microservices architecture, or orchestrating interactions within AI agent swarms, NetworkKit provides the necessary tools and abstractions to ensure reliable and efficient communication.

## Features

- **Message Handling**: Define and manage various message types and content, including support for multiple message categories such as chat, system updates, and sensor data.
- **ZeroMQ Integration**: Efficiently publish and subscribe to messages using ZeroMQ, enabling high-performance asynchronous communication.
- **HTTP Integration**: Send and receive messages via HTTP using FastAPI, providing flexibility for web-based communication.
- **Pydantic Models**: Leverage Pydantic for robust data validation and serialization, ensuring data integrity and ease of use.
- **Asynchronous Communication**: Support for asynchronous message handling, allowing non-blocking operations and improved performance in distributed systems.
- **Extensible Architecture**: Easily extend the framework to support additional protocols or custom message handling logic.

## Installation

To install NetworkKit, use pip to install the package:

```bash
pip install networkkit
```

## Usage

### Message Data Structures

The `messages.py` module defines the message data structures used for communication within the NetworkKit framework.

#### Message

The `Message` class is a Pydantic model representing a message object exchanged through the data bus. It provides a structured way to define and validate the content of messages, ensuring consistency and reliability in communication.

Attributes:
- `source` (str): The source of the message (e.g., agent name, sensor name).
- `to` (str): The intended recipient of the message (e.g., agent name, or 'ALL' for broadcast).
- `content` (str): The actual message content in string format.
- `created_at` (str, optional): The timestamp of when the message was created. If not provided, it will be automatically set to the current time by the databus.
- `message_type` (MessageType): The type of message as defined by the `MessageType` enumeration.

Example:
```python
from networkkit.messages import Message, MessageType

message = Message(
    source="Agent1",
    to="Agent2",
    content="Hello, Agent2!",
    message_type=MessageType.CHAT
)
```

#### MessageType

An enumeration class representing the different message types used in NetworkKit:

- `HELO`: Indicates a login request or checking if the agent “to” is available.
- `ACK`: Response to a HELO request, indicating the agent is available.
- `CHAT`: Text message intended for conversation.
- `SYSTEM`: System message coming from the data hub.
- `SENSOR`: Messages for data coming from sensors.
- `ERROR`: Error messages.
- `INFO`: A communication for agents on any non conversational or sensor data.

### Network Module

The `network.py` module provides interfaces and implementations for message sending and receiving.

#### Subscriber Protocol

Defines the interface for subscribers to the bus that can recieve and handle Messages. Subscribers must implement the following methods:

- `handle_message(self, message: Message) -> Any`: Asynchronous method for handling received messages.
- `is_intended_for_me(self, message: Message) -> bool`: Method to determine if a message is intended for this subscriber.

#### MessageSender Protocol

Defines the interface for senders that send messages over the network. Implementations must provide the following method:

- `send_message(self, message: Message) -> Any`: Method to send a message over the network.

#### ZMQMessageReceiver

Class to receive messages using ZeroMQ and distribute them to registered subscribers. Implementors of the Suscriber protocol can register to subscribe to this receiver. It establishes a ZeroMQ subscriber socket, listens for messages, and distributes them to registered subscribers based on their `is_intended_for_me` method.

#### HTTPMessageSender

Class to send messages over HTTP using the `requests` library. It sends messages as JSON payloads to a specified HTTP endpoint.

### Data Bus

The `databus.py` module provides a data bus service for publishing messages using ZeroMQ and a FastAPI interface (HTTP) for receiving messages. Incoming Messages via the HTTP endpoint will be published via the ZeroMQ for subscribers to pick up.

#### Running the Data Bus

1. Execute the script from the console:

```bash
python -m networkkit.databus
```

This will start the FastAPI server and the ZeroMQ publisher.


## Example Code

Here is an example of how to use NetworkKit to send and receive messages:
```python
from networkkit.messages import Message, MessageType
from networkkit.network import ZMQMessageReceiver, HTTPMessageSender

# Example message
message = Message(
    source="Agent1",
    to="Agent2",
    content="Hello, Agent2!",
    message_type=MessageType.CHAT
)

# Sending a message over HTTP
sender = HTTPMessageSender(publish_address="http://127.0.0.1:8000")
response = sender.send_message(message)
print(response.status_code)

# Receiving messages with ZMQ
receiver = ZMQMessageReceiver(subscribe_address="tcp://127.0.0.1:5555")

class MySubscriber:
    name = "Agent2"

    async def handle_message(self, message: Message):
        print(f"Received message: {message.content}")

    def is_intended_for_me(self, message: Message) -> bool:
        return message.to == self.name or message.to == "ALL"

receiver.register_subscriber(MySubscriber())
asyncio.run(receiver.start())
```

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the terms of the MIT license. See the LICENSE file for details.

## Authors
	•	Vikram Kumar - vik@japanvik.net
