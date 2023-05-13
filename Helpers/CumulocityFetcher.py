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

        while "managedObjects" in resp and len(resp["managedObjects"]) is not 0:
            devices += resp["managedObjects"]
            resp = requests.request("GET", resp["next"], headers=self.headers).json()
        return devices

    def get_all_measurements_ranged(self, date_from, date_to):
        measurements = []
        resp = requests \
            .request("GET", self.__url("/measurement/measurements",
                                       "withTotalPages=true",
                                       "pageSize=2000",
                                       f"dateFrom={date_from}",
                                       f"dateTo={date_to}"), headers=self.headers) \
            .json()

        print(resp["statistics"])

        current_page = 1
        while "measurements" in resp and len(resp["measurements"]) is not 0:
            measurements += resp["measurements"]
            resp = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           "pageSize=2000",
                                           f"currentPage={current_page}",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.headers) \
                .json()
            current_page += 1
        return measurements

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

    def get_measurement_series(self, date_from, date_to):
        resp = requests.request("GET",
                                self.__url(f"/measurement/measurements/series",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"),
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

    def get_device_by_name(self, name):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects", f"query=$filter=(name+eq+'*{name}*')",
                                           "pageSize=100", "withParents=true"),
                                headers=self.headers)
        # Additional checks
        additional_check = requests.request("GET",
                                            self.__url(f"/inventory/managedObjects", f"q=$filter=(name+eq+'*{name}*')",
                                                       "pageSize=100"),
                                            headers=self.headers)

        body_json_with_parent = resp.json()
        body_json_with_parent = [x for x in body_json_with_parent['managedObjects'] if
                                 len(x['deviceParents']['references']) > 0]

        body_json_additional_check = additional_check.json()
        body_json_additional_check = [x for x in body_json_additional_check['managedObjects']]

        body_json_with_parent += body_json_additional_check

        resp._content = dumps(body_json_with_parent).encode()

        return resp.json()

    def get_device_by_name_wrong(self, name):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects", f"query=$filter=(name+eq+'{name}')",
                                           "pageSize=100", "withParents=true"),
                                headers=self.headers)

        return resp.json()

    def get_device_by_id(self, id_):
        resp = requests.request("GET",
                                self.__url(f"/inventory/managedObjects", f"query=$filter=(id+eq+'{id_}')",
                                           "pageSize=100"),
                                headers=self.headers)

        resp_json = resp.json()
        resp_json = resp_json['managedObjects']
        resp._content = json.dumps(resp_json).encode()
        return resp.json()

    def __url(self, path, *args):
        if len(args) > 0:
            final_url = f"https://{self.base_url}{path}/?"
            for arg in args:
                final_url += f"{arg}&"
            return final_url.strip('&')
        else:
            return f"https://{self.base_url}{path}"
