import logging
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
from threading import Thread

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class ContextFilter(logging.Filter):

    """
    A custom logging filter that adds context information to log messages.
    Args:
        space (str): The configuration space.
    Methods:
        filter(record): Adds the configuration space to the log record.
    Attributes:
        space (str): The configuration space.

    """

    def __init__(self, space, entrypoint):
        super().__init__()
        self.space = space
        self.entrypoint = entrypoint

    def filter(self, record):
        record.entrypoint = self.entrypoint
        record.space = self.space
        return True


class SlackHandler(logging.Handler):

    """
    A custom logging handler that sends log messages to a Slack channel.
    Args:
        token (str): The Slack API token.
        channel (str): The name or ID of the Slack channel to send the log messages to.
    Attributes:
        client (WebClient): The Slack client.
        channel (str): The name or ID of the Slack channel.
        send_interval (int): Time interval in seconds to send logs.
        max_message_length (int): Maximum length of a message.
        message_queue (Queue): A queue to store log messages.
        running (bool): Whether the background thread is running.
        worker_thread (Thread): A background thread to send logs periodically.
    Methods:
        emit(record): Adds a log message to the queue.
        _send_logs_periodically(): Sends logs to Slack periodically.
        _send_messages_to_slack(messages): Sends a list of log messages to Slack.
        flush(): Sends all log messages in the queue to Slack.
        close(): Stops the background thread and sends any remaining logs to Slack
    Raises:
        SlackApiError: If there is an error sending the log message to Slack.
    """

    def __init__(self, token, channel):
        super().__init__()
        self.client = WebClient(token=token)
        self.channel = channel

        self.send_interval = 1  # Time interval in seconds to send logs
        self.max_message_length = 35000  # Maximum length of a message
        self.message_queue = Queue()
        self.running = True

        # Start a background thread to send logs periodically
        self.worker_thread = Thread(target=self._send_logs_periodically)
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def emit(self, record):

        """
        Adds a log message to the queue.
        """

        if record.name != "bagit":
            log_entry = self.format(record)
            self.message_queue.put(log_entry)

    def _send_logs_periodically(self):

        """
        Flushes the message queue periodically.
        """

        while self.running:
            time.sleep(self.send_interval)
            self.flush()

    def _send_messages_to_slack(self, messages):

        """
        Sends a list of log messages to Slack.

        Args:
            messages (list): A list of log messages.
        """

        message = "\n".join(messages)
        try:
            self.client.chat_postMessage(channel=self.channel, text=message)
        except SlackApiError as e:
            print(f"Failed to send log to Slack: {e.response['error']}")

    def flush(self):

        """
        Sends log messages in the queue to Slack.
        """

        messages = []
        current_length = 0
        while not self.message_queue.empty():
            message = self.message_queue.get()
            if current_length + len(message) + 1 > self.max_message_length:  # +1 for newline
                self._send_messages_to_slack(messages)
                messages = [message]
                current_length = len(message)
            else:
                messages.append(message)
                current_length += len(message) + 1  # +1 for newline

        if messages:
            self._send_messages_to_slack(messages)

    def close(self):

        """
        Stops the background thread and sends any remaining logs to Slack.
        """

        self.running = False
        self.worker_thread.join()
        self.flush()
        super().close()

class IngestLogger:

    """
    A custom logger class that sets up logging for the preservation ingest job.
    Args:
        debug (bool): Whether to log debug messages.
        logfile (str): The path to the log file.
        rotation_days (int): The number of days to keep log files.
        backup_count (int): The number of log files to keep.
        slack_token (str): The Slack API token.
        slack_channel (str): The name or ID of the Slack channel to send the log messages to.
        config_space (str): The configuration space.
        entrypoint (str): The entrypoint script for the ingest job.
    Methods:
        setup_logging(): Sets up logging for the preservation ingest job.
    """

    def __init__(self, **kwargs):
        self.debug = kwargs.get("debug", False)
        self.logfile = kwargs.get("logfile", None)
        self.rotation_days = kwargs.get("rotation_days", 30)
        self.backup_count = kwargs.get("backup_count", 10)
        self.slack_token = kwargs.get("slack_token", None)
        self.slack_channel = kwargs.get("slack_channel", None)
        self.config_space = kwargs.get("config_space", "DEV")
        self.entrypoint = kwargs.get("entrypoint", "NULL")

    def setup_logging(self):

        """Set up logging for the preservation ingest job."""

        log_level  = logging.DEBUG if self.debug else logging.INFO
        formatter = logging.Formatter('%(asctime)s - %(entrypoint)s - %(space)s - %(name)s - %(levelname)s - %(message)s')
        extra_filter = ContextFilter(self.config_space,  self.entrypoint)

        logger = logging.getLogger()
        logger.setLevel(log_level)
        logger.addFilter(extra_filter)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        handler.setFormatter(formatter)
        handler.addFilter(extra_filter)
        logger.addHandler(handler)

        file_handler = TimedRotatingFileHandler(self.logfile, when='D', interval=self.rotation_days, backupCount=self.backup_count)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(extra_filter)
        logger.addHandler(file_handler)

        if self.slack_token and self.slack_channel:
            slack_handler = SlackHandler(self.slack_token, self.slack_channel)
            slack_handler.setLevel(logging.INFO)
            slack_handler.setFormatter(formatter)
            slack_handler.addFilter(extra_filter)
            logger.addHandler(slack_handler)

        return logger
