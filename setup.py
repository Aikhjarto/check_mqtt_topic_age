from distutils.core import setup
from setuptools import find_packages

setup(name="check_mqtt_topic_age",
      version='0.1',
      description='Nagios/Ncinga plugin to check when an MQTT topic was published',
      author='Thomas Wagner',
      author_email='wagner-thomas@gmx.at',
      url='https://github.com/Aikhjarto/check_mqtt_topic_age',
      packages=find_packages(where='src'),
      package_dir={"": "src"},
      install_requires=["paho-mqtt"],
      )
