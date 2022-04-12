import paho.mqtt.client as mqtt
import time, signal, urllib, json, requests
import traceback, os

from arrow import Arrow as arrow
from confirmation_threshold import confirmation_threshold
from daemon import Daemon
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import *
from multitimer import MultiTimer
from requests_oauthlib import OAuth1 as oauth

class TweeterDaemon(Daemon):
    def run(self):
        tw = Tweeter()
        my_path = os.path.dirname(os.path.abspath(__file__))
        config = open(my_path + "/tweeter.json", "r")
        tw.data = Tweeter.data.from_json(config.read())
        config.close()

        tw.run()

class Tweeter(mqtt.Client):
    """If only this daemon could automate cat picture posting."""

    version = '2020'

    @dataclass_json
    @dataclass
    class data:
        description: str
        listen_topics: List[str]
        listen_channel: str
        enumeration: Dict[str, str]
        message: str
        timezone: str
        time_format: str
        confirmation_interval: int
        confirmation_time: int
        confirmation_updates: int
        tweet_timeout: int
        twitter_consumer_key: str
        twitter_consumer_secret: str
        twitter_access_key: str
        twitter_access_secret: str
        pidfile: str
        mqtt_broker: str
        mqtt_port: int
        mqtt_timeout: int

    def on_log(self, client, userdata, level, buff):
        if level != mqtt.MQTT_LOG_DEBUG:
            print (buff)
        if level == mqtt.MQTT_LOG_ERR:
            print ("Fatal Error Encountered:")
            traceback.print_exc()
            os._exit(1)

    def on_connect(self, client, userdata, flags, rc):
        print("Connected: " + str(rc))
        for topic in self.data.listen_topics:
            print("Subscribing to " + topic)
            self.subscribe(topic)

    def on_message(self, client, userdata, message):
        print("Received: " + message.topic)
        print(message.payload.decode('utf-8'))
        rcvd = json.loads(message.payload.decode('utf-8'))
        if self.data.listen_channel in rcvd:
            self.latched_value = str(rcvd[self.data.listen_channel])
            self.pre_latch_notif = 0
        
    def tweet(self, message):
        print("Tweeting message: " + message)
        timeline_endpoint = "https://api.twitter.com/1.1/statuses/update.json?status="
        timeline_endpoint += urllib.parse.quote(message, safe='')
        try:
            response = requests.post(timeline_endpoint, auth=self.twitter_oauth)

            # JSON printing code
            parsed_json = json.loads(response.text)
            print (json.dumps(parsed_json, indent=4))

            if response.status_code >= 200 and response.status_code < 300:
                print ("Tweet success")
            else:
                print ("Tweet failed with error code:", response.status_code)
        except ValueError as e:
            print ("Tweet failed:", e)

    # evaluate confirmation time
    def evaluate_ct(self):
        try:
            print("Evaluating confirmation time (ECT), pre-notification at " + str(self.pre_latch_notif) + " out of " + str(self.data.confirmation_updates))
            # reset the timer if no notifications received for a while
            if (self.pre_latch_notif >= self.data.confirmation_updates):
                print("ECT: Reset confirmation time.")
                self.ct.time = 2
                self.ct.holdoff = False
            result = self.ct.update(self.latched_value)
            self.pre_latch_notif += 1
            if self.ct.holdoff:
                announcement = str((self.ct.delay - self.ct.time)*self.data.confirmation_interval) + " until " + self.data.enumeration[self.latched_value]
                print("ECT: in holdoff, publishing time announcement: " + announcement)
                self.publish("tweeter/time_announce", announcement)
            else:
                announcement = "is " + self.data.enumeration[result[1]]
                print("ECT: new value or value restored: " + announcement)
                self.publish("tweeter/time_announce", announcement)
            if result[0]:
                attime = arrow.fromtimestamp(time.time()).to(self.data.timezone).strftime(self.data.time_format)
                self.tweet(self.data.message.format(self.data.enumeration[result[1]], attime))
        except:
            traceback.print_ext()
            os._exit(1)

    def signal_handler(self, signum, frame):
        print("Caught a deadly signal!")
        self.running = False

    def bootup(self):
        print("Bootup Delay")
        self.latched_value = next(iter(self.data.enumeration))
        self.ct = confirmation_threshold(self.latched_value, self.data.confirmation_time)
        self.pre_latch_notif = 0
        self.twitter_oauth = oauth(client_key=self.data.twitter_consumer_key,
            client_secret=self.data.twitter_consumer_secret,
            resource_owner_key=self.data.twitter_access_key,
            resource_owner_secret=self.data.twitter_access_secret)
        self.twitter_oauth.timeout = self.data.tweet_timeout
        time.sleep(60);
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.running = True

    def run(self):
        print("Haldor Tweeter version " + self.version + " starting") 
        self.connect(self.data.mqtt_broker, self.data.mqtt_port, self.data.mqtt_timeout)
        self.bootup()
        timer = MultiTimer(interval=self.data.confirmation_interval, function=self.evaluate_ct)
        timer.start()

        while self.running: 
            self.loop()

        timer.stop()
        exit(0)
