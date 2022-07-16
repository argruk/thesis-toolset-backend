from base64 import b64encode
import requests
import json
from json import dumps

class CumulocityFetcher:
    def __init__(self, username, password, platform):
        credentials = b64encode(bytes(username + ':' + password, "utf-8")).decode("ascii")
        self.headers = {'Authorization': 'Basic %s' % credentials}
        self.base_url = platform

    def get_all_devices(self):
        devices = []
        resp = requests \
            .request("GET", self.__url("/inventory/managedObjects", "fragmentType=c8y_IsDevice"), headers=self.headers) \
            .json()

        while len(resp["managedObjects"]) is not 0:
            devices += resp["managedObjects"]
            resp = requests.request("GET", resp["next"], headers=self.headers).json()
        return devices

    def get_single_device(self, id_):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects/{id_}"),
                                headers=self.headers)
        return resp.json()

    def get_measurements_for_device(self, id_, type, pageSize):
        resp = requests.request("GET",
                                self.__url(f"/measurement/measurements",
                                           f"source={id_}",
                                           f"type={type}",
                                           f"pageSize={pageSize}",
                                           "withTotalPages=true",
                                           "revert=true",
                                           "dateFrom=2022-7-6"),
                                headers=self.headers)
        return resp.json()

    def get_measurement_types_for_device(self, id_):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects/{id_}/supportedMeasurements"),
                                headers=self.headers)
        body_json = resp.json()
        body_json = body_json['c8y_SupportedMeasurements']
        resp._content = dumps(body_json).encode()
        return resp.json()

    def get_device_children(self, id_):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects/{id_}/childDevices", "pageSize=100"),
                                headers=self.headers)
        return resp.json()

    def __url(self, path, *args):
        if len(args) > 0:
            final_url = f"https://{self.base_url}{path}/?"
            for arg in args:
                final_url+= f"{arg}&"
            return final_url.strip('&')
        else:
            return f"https://{self.base_url}{path}"
