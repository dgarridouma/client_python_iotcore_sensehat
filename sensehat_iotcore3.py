# Google IoT Core console client using SenseHat Emulator
__author__ = 'dgarrido'

DEVICE_ID='mi-dispositivo'
PROJECT_ID='YOUR_PROJECT_ID_HERE'
CLOUD_REGION='europe-west1'
REGISTRY_ID='mi-registro'
PRIVATE_KEY_FILE='./rsa_private.pem'
ALGORITHM='RS256'
CA_CERTS='roots.pem'
MQTT_BRIDGE_HOSTNAME='mqtt.googleapis.com'
MQTT_BRIDGE_PORT=8883

import datetime
import ssl
import time
import jwt
import paho.mqtt.client as mqtt
import json

from sense_emu import SenseHat
from threading import Lock

sense = SenseHat() 
period = 10
blue = (0, 0, 255)
yellow = (255, 255, 0)
lock = Lock()

def create_jwt(project_id, private_key_file, algorithm):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
        Args:
         project_id: The cloud project ID this device belongs to
         private_key_file: A path to a file containing either an RSA256 or
                 ES256 private key.
         algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
        Returns:
            An MQTT generated from the given project_id and private key, which
            expires in 60 minutes. After 60 minutes, your client will be
            disconnected, and a new JWT will have to be generated.
        Raises:
            ValueError: If the private_key_file does not contain a known key.
        """

    token = {
            # The time that the token was issued at
            'iat': datetime.datetime.utcnow(),
            # The time the token expires.
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            # The audience field should always be set to the GCP project id.
            'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)


def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print('on_connect', mqtt.connack_string(rc))


def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print('on_disconnect', error_str(rc))


def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    print('on_publish')


def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload,"utf-8")
    #print('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
    #        payload, message.topic, str(message.qos)))
    if message.topic.endswith('commands'):
        global period

        dict_command=json.loads(payload)
        period = int(dict_command['period'])
        lock.acquire()
        sense.show_message(dict_command['message'], text_colour=yellow, back_colour=blue, scroll_speed=0.05)
        lock.release()


def get_client(
        project_id, cloud_region, registry_id, device_id, private_key_file,
        algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client = mqtt.Client(
            client_id=('projects/{}/locations/{}/registries/{}/devices/{}'
                       .format(
                               project_id,
                               cloud_region,
                               registry_id,
                               device_id)))

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
            username='unused',
            password=create_jwt(
                    project_id, private_key_file, algorithm))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(device_id)

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)

    # The topic that the device will receive commands on.
    mqtt_command_topic = '/devices/{}/commands/#'.format(device_id)

    # Subscribe to the commands topic, QoS 1 enables message acknowledgement.
    print('Subscribing to {}'.format(mqtt_command_topic))
    client.subscribe(mqtt_command_topic, qos=0)

    return client



def main():
    mqtt_topic = '/devices/{}/events'.format(DEVICE_ID)

    client = get_client(
        PROJECT_ID, CLOUD_REGION, REGISTRY_ID, DEVICE_ID,
        PRIVATE_KEY_FILE, ALGORITHM, CA_CERTS,
        MQTT_BRIDGE_HOSTNAME, MQTT_BRIDGE_PORT)

    client.loop_start()
    while True:
        msg=dict()
        msg['temperature']=sense.temperature
        msg['humidity']=sense.humidity
        msg['pressure']=sense.pressure
        msg['timestamp']=datetime.datetime.now()
        json_data=json.dumps(msg,default=str)
        lock.acquire()
        sense.show_message("{:.2f}".format(sense.temperature)+' '+"{:.2f}".format(sense.pressure)+' '+"{:.2f}".format(sense.humidity), text_colour=yellow, back_colour=blue, scroll_speed=0.05)
        lock.release()
        print(str(sense.temperature)+' '+str(sense.pressure)+' '+str(sense.humidity))
        
        client.publish(mqtt_topic, json_data, qos=1)
        time.sleep(period)

if __name__ == '__main__':
    main()
