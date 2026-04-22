#!/usr/bin/env python3

import argparse
import json
import logging
import signal
import sys
import time
import sqlite3
import threading
from typing import Dict, Any, Optional, List, Tuple
import socket
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
class LocalBuffer:    
    def __init__(self, db_path: str = "log_buffer.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_table()
    
    def _create_table(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                attempts INTEGER DEFAULT 0
            )
        """)
        self.conn.commit()
    
    def store_log(self, log: Dict[str, Any]):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pending_logs (log_json, created_at) VALUES (?, ?)",
            (json.dumps(log), time.time())
        )
        self.conn.commit()
        logger.debug(f"Log saved to local buffer")
    
    def get_pending_logs(self, limit: int = 100) -> List[Tuple[int, str]]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, log_json FROM pending_logs ORDER BY created_at LIMIT ?",
            (limit,)
        )
        return cursor.fetchall()
    
    # Remove sent log from buffer
    def remove_log(self, log_id: int):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM pending_logs WHERE id = ?", (log_id,))
        self.conn.commit()
    
    def increment_attempt(self, log_id: int):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE pending_logs SET attempts = attempts + 1 WHERE id = ?",
            (log_id,)
        )
        self.conn.commit()
    
    def close(self):
        self.conn.close()


# Class for sending logs
class LoggingClient:    
    def __init__(self, rabbitmq_url: str, source_ip: str, source_tag: str):
        self.rabbitmq_url = rabbitmq_url
        self.source_ip = source_ip
        self.source_tag = source_tag
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.buffer = LocalBuffer()
        self.running = True
        self.reconnect_delay = 2
        self.max_reconnect_delay = 30
        
        # Background thread for processing buffer
        self.buffer_thread = threading.Thread(target=self._process_buffer_loop, daemon=True)
        self.buffer_thread.start()
    
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
            
        except (AMQPConnectionError, ConnectionClosed, ChannelError) as e:
            logger.error(f"Error connecting to RabbitMQ: {e}")
            return False
    
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
        
        log_json = json.dumps(full_log)
        
        # Try to send
        try:
            if not self.connection or not self.connection.is_open:
                raise ConnectionError("No connection with RabbitMQ")
            
            self._send_message(log_json)
            logger.info(f"Log sent: {full_log['source_tag']} @ {full_log['timestamp']}")
            
        except Exception as e:
            logger.warning(f"Could not send log; saving to buffer: {e}")
            self.buffer.store_log(full_log)
    
    def _flush_buffer(self):
        pending_logs = self.buffer.get_pending_logs()
        if not pending_logs:
            return
        
        logger.info(f"Attempting to send {len(pending_logs)} logs from buffer")
        
        for log_id, log_json in pending_logs:
            try:
                self._send_message(log_json)
                self.buffer.remove_log(log_id)
                logger.info(f"Log {log_id} sent from buffer")
            except Exception as e:
                logger.error(f"Could not send log {log_id} from buffer: {e}")
                self.buffer.increment_attempt(log_id)
                break
    
    # Attempt to flush buffer perodically
    def _process_buffer_loop(self):
        while self.running:
            time.sleep(10)  # every 10 seconds
            if self.connection and self.connection.is_open:
                self._flush_buffer()
    
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
                    time.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                    continue
            
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
        if self.connection and self.connection.is_open:
            self.connection.close()
        self.buffer.close()
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
        sys.exit(1)


if __name__ == "__main__":
    main()