import asyncio
import datetime
from logging import DEBUG, INFO
import uvicorn
import zmq
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from networkkit.messages import Message, MessageType
from pydantic import ValidationError

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(INFO)

"""
This module provides a data bus service for publishing messages using ZeroMQ and a FastAPI interface for receiving messages.

The core functionality is provided by the `send_message` function, which sends a message object to a ZeroMQ publisher channel. It also offers a FastAPI endpoint (`/data`) to receive messages via an HTTP POST request.

Additionally, this module includes a background task (`time_publisher_task`) that publishes system messages containing the current time every 5 minutes.

**To run the databus:**

1. (Optional) Activate your virtual environment (if applicable).
2. Execute the script from the console: `python -m "networkkit.databus"`
"""

# ZeroMQ Publisher setup
context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5555")  # Allow connections from any IP address

async def send_message(message: Message):
    try:
        # Make sure we have a created_at
        if not message.created_at:
            message.created_at = datetime.datetime.now().isoformat()
        # Send the message to the ZeroMQ publisher channel in JSON format
        publisher.send_json(message.model_dump())

        # Return success status
        return {"status": "success"}

    except Exception as e:
        # Log the exception
        logger.error(f"Error sending message: {e}")
        # Return error status
        return {"status": "error"}

async def time_publisher_task():
    while True:
        current_time = datetime.datetime.now().isoformat()
        message = Message(
            source="TimePublisher",
            to="ALL",
            content=f"Current time: {current_time}",
            created_at=current_time,
            message_type=MessageType.SYSTEM
        )
        await send_message(message)
        await asyncio.sleep(300)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Launch the time publisher
    """
    asyncio.create_task(time_publisher_task())
    yield

app = FastAPI(lifespan=lifespan)

# End points
@app.post("/data")
async def post_message(request: Request):
    try:
        # Log the received message
        body = await request.json()
        logger.debug(f"Received request body: {body}")
        if isinstance(body, dict):
            message = Message(**body)
        else:
            logger.error("Request body is not a valid dictionary.")
            return {"status": "error", "detail": "Invalid request format."}
    except ValidationError as e:
        logger.error(f"Validation error: {e.json()}")
        return {"status": "error", "detail": e.errors()}
    except Exception as e:
        logger.error(f"Unexpected error during message parsing: {e}")
        return {"status": "error", "detail": str(e)}
    
    result = await send_message(message)
    logger.debug(f"Send message result: {result}")
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)