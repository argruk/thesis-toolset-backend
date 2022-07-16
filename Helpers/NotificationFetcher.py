from base64 import b64encode
import requests
import json

class NotificationFetcher:
    def __init__(self, username, password, platform):
        credentials = b64encode(bytes(username + ':' + password, "utf-8")).decode("ascii")
        self.headers = {'Authorization': 'Basic %s' % credentials}
        self.base_url = platform
        self.token = ""

    def create_token(self):
        payload = {
            "subscriber": "testSubscriber",
            "subscription": "testSubscription",
            "expiresInMinutes": 90
            }
        response = requests.request("POST", self.__url("/notification2/token"), headers=self.headers, data=json.dumps(payload)).json()
        return response

    def add_subscription(self, device_id, type):
        payload = {
            "source": {
                "id": str(device_id)
            },
            "context": "mo",
            "subscription": "testSubscription",
            "subscriptionFilter": {
                "typeFilter": type
            }
        }
        print(payload)
        response = requests.request("POST", self.__url("/notification2/subscriptions"), headers=self.headers,
                                    data=json.dumps(payload)).json()
        return response

    def __url(self, path, *args):
        if len(args) > 0:
            final_url = f"https://{self.base_url}{path}/?"
            for arg in args:
                final_url+= f"{arg}&"
            return final_url.strip('&')
        else:
            return f"https://{self.base_url}{path}"