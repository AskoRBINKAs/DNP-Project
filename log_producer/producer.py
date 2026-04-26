#!/usr/bin/env python3

import argparse
import json
import logging
import signal
import sys
import time
import threading
from typing import Dict, Any, Optional, List
import socket
import os
import tempfile

import pika
from pika.exceptions import AMQPConnectionError, ChannelError, ConnectionClosed

# Producer's own logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Buffer for times when RabbitMQ isn't available
class LocalFileBuffer:
    def __init__(self, file_path: str = "temp.log"):
        self.file_path = file_path
        self.lock = threading.Lock()
        open(self.file_path, "a", encoding="utf-8").close()

    def store_log(self, log: Dict[str, Any]):
        with self.lock:
            with open(self.file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(log, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())

        logger.debug("Log saved to local buffer")

    def get_pending_logs(self) -> List[str]:
        with self.lock:
            if not os.path.exists(self.file_path):
                return []

            with open(self.file_path, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip()]

    # Remove sent log from buffer
    def rewrite_logs(self, remaining_logs: List[str]):
        with self.lock:
            directory = os.path.dirname(os.path.abspath(self.file_path)) or "."
            fd, tmp_path = tempfile.mkstemp(
                prefix="temp_log_",
                suffix=".tmp",
                dir=directory
            )

            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
                    for line in remaining_logs:
                        tmp_file.write(line + "\n")

                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())

                os.replace(tmp_path, self.file_path)

            except Exception:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                raise


# Class for sending logs
class LoggingClient:
    def __init__(self, rabbitmq_url: str, source_ip: str, source_tag: str):
        self.rabbitmq_url = rabbitmq_url
        self.source_ip = source_ip
        self.source_tag = source_tag
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.buffer = LocalFileBuffer("temp.log")
        self.running = True
        self.reconnect_delay = 2
        self.max_reconnect_delay = 30

    def connect(self):
        try:
            params = pika.URLParameters(self.rabbitmq_url)
            params.heartbeat = 60
            params.blocked_connection_timeout = 300

            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()

            # Logs queue
            self.channel.queue_declare(
                queue='logs',
                durable=True,
                auto_delete=False
            )

            logger.info("Connected to RabbitMQ; logs queue ready")
            self.reconnect_delay = 2

            # In case there are logs in buffer
            self._flush_buffer()

            return True

        except (AMQPConnectionError, ConnectionClosed, ChannelError, OSError) as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            self._close_connection()
            return False

    def _close_connection(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

        self.connection = None
        self.channel = None

    def _send_message(self, message: str):
        if not self.channel or not self.channel.is_open:
            raise ConnectionError("No active channel found")

        self.channel.basic_publish(
            exchange='',  # default exchange
            routing_key='logs',  # name of queue
            body=message.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent message
                content_type='application/json'
            )
        )
        logger.debug(f"Message sent: {message[:100]}...")

    def send_log(self, log_data: Dict[str, Any]):
        full_log = {
            "source_ip": self.source_ip,
            "source_tag": self.source_tag,
            "timestamp": log_data.get("timestamp", int(time.time())),
            "data": log_data.get("data", {})
        }

        log_json = json.dumps(full_log, ensure_ascii=False)

        # Try to send
        try:
            if not self.connection or not self.connection.is_open:
                raise ConnectionError("No connection with RabbitMQ")

            self._send_message(log_json)
            logger.info(f"Log sent: {full_log['source_tag']} @ {full_log['timestamp']}")

        except Exception as e:
            logger.warning(f"Could not send log; saving to buffer: {e}")
            self.buffer.store_log(full_log)
            self._close_connection()

    def _flush_buffer(self):
        pending_logs = self.buffer.get_pending_logs()
        if not pending_logs:
            return

        logger.info(f"Attempting to send {len(pending_logs)} logs from buffer")

        remaining_logs: List[str] = []

        for index, log_json in enumerate(pending_logs):
            try:
                json.loads(log_json)
                self._send_message(log_json)
                logger.info(f"Log {index + 1} sent from buffer")
            except Exception as e:
                logger.error(f"Could not send log {index + 1} from buffer: {e}")
                remaining_logs = pending_logs[index:]
                self._close_connection()
                break

        self.buffer.rewrite_logs(remaining_logs)

    def generate_test_log(self) -> Dict[str, Any]:
        import random

        # Random JSON
        data = {
            "cpu_usage": round(random.uniform(0, 100), 2),
            "memory_mb": random.randint(256, 16384),
            "status": random.choice(["OK", "WARNING", "ERROR"]),
            "message": f"Test log from {self.source_tag}"
        }

        return {
            "timestamp": int(time.time()),
            "data": data
        }

    def run(self, interval_seconds: int = 5):
        # Handle signal
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(f"Client started: source_ip={self.source_ip}, source_tag={self.source_tag}")

        while self.running:
            # Check and recover connection
            if not self.connection or not self.connection.is_open:
                logger.info("Trying to connect to RabbitMQ...")
                if self.connect():
                    logger.info("Connection restored")
                else:
                    logger.warning("RabbitMQ is not available; generated logs will be saved to buffer")

            test_log = self.generate_test_log()
            self.send_log(test_log)

            for _ in range(interval_seconds):
                if not self.running:
                    break
                time.sleep(1)

        self.cleanup()

    def _signal_handler(self, signum, frame):
        logger.info("Signal received, shutting down...")
        self.running = False

    def cleanup(self):
        logger.info("Cleanup...")
        self._close_connection()
        logger.info("Client stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--ip', required=True,
        help="Source IP address"
    )
    parser.add_argument(
        '--tag', required=True,
        help="Source tag"
    )
    parser.add_argument(
        '--interval', type=int, default=5,
        help="Logs interval (seconds)"
    )
    parser.add_argument(
        '--rabbitmq-url',
        required=True,
        help="URL for connecting to RabbitMQ"
    )

    args = parser.parse_args()

    # Validate IP
    try:
        socket.inet_aton(args.ip)
    except socket.error:
        logger.error(f"Invalid IP address: {args.ip}")
        sys.exit(1)

    # Start client
    client = LoggingClient(
        rabbitmq_url=args.rabbitmq_url,
        source_ip=args.ip,
        source_tag=args.tag
    )

    try:
        client.run(interval_seconds=args.interval)
    except KeyboardInterrupt:
        logger.info("User interrupt")
        client.cleanup()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        client.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()