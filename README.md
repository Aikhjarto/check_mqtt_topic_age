# check_mqtt_topic_age
This project provides a daemon subscribing to mqtt topics and logging their 
most recent time-of-arrival, 
as well as a nagios/ncinga compatible check script to the age of the topics.

# Usage
Run `python3 -m mqtt_message_timestamp_logger` in background, e.g. via systemd.
An example for a service can be found in the folder `data`.
It will populate a database with the most recent arrival times of messages 
for all observed topics.

Then use `python3 -m check_mqtt_topic_age` to check for those timestamps.
The output of `python3 -m check_mqtt_topic_age` is compatible with nagios/ncinga.


# Disclaimer
This project is for small scale usage. If you have a high rate of messages you 
want to monitor, this python implemenation might be too slow.
Consider using an extension to your mqtt broker in that case.

