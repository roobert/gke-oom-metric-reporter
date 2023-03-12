#!/usr/bin/env python
#
# Derived from: https://github.com/AckeeCZ/terraform-gcp-gke-oom-kill-monitoring/blob/master/code/check_oom_killed_pods.py
#
# Added support to scan multiple namespaces in an async way

import asyncio
import os
import ssl
import time
import urllib.request

import aiohttp
from google.cloud import monitoring_v3
from kubernetes import client, config

config.load_incluster_config()


INTERVAL = os.environ.get("INTERVAL", "60")

APISERVER = "https://kubernetes.default.svc"
SERVICEACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"


async def main():
    project_id = gcp_project_id()
    cluster_location = _cluster_location()
    cluster_name = _cluster_name()

    print("==> starting oom metric service")

    while True:
        print("==> creating tasks for each namespace")
        tasks = []

        # FIXME: this fails as collection..?
        for namespace in namespaces():
            tasks.append(
                asyncio.create_task(
                    check_oom(project_id, cluster_location, cluster_name, namespace)
                )
            )

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for response in responses:
            print("--")
            print(response)

        print("==> completed iteration")

        time.sleep(int(INTERVAL))


def gcp_project_id():
    return metadata("project/project-id")


def _cluster_location():
    # convert zone to region
    # FIXME: this probably needs error handling..
    # return "-".join(metadata("instance/zone").split("/")[-1].split("-")[:2])
    return metadata("instance/attributes/cluster-location")


def _cluster_name():
    return metadata("instance/attributes/cluster-name")


def metadata(path):
    url = f"http://metadata.google.internal/computeMetadata/v1/{path}"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    project_id = urllib.request.urlopen(req).read().decode()
    return project_id


# FIXME: work out namespaces
def namespaces():
    v1 = client.CoreV1Api()
    response = v1.list_namespace(watch=False, pretty=True)
    return [item.metadata.name for item in response.items]


async def check_oom(project_id, cluster_location, cluster_name, namespace):
    print(
        f"==> creating task: {project_id}/{cluster_location}/{cluster_name}/{namespace}"
    )
    response = await async_request(namespace)

    for item in response["items"]:
        container_statuses = item["status"].get("containerStatuses", [])
        if not len(container_statuses):
            continue

        last_state = container_statuses[0].get("lastState")
        if last_state is {}:
            continue

        for k, v in last_state.items():
            if k == "terminated":
                if v["reason"] != "OOMKilled":
                    continue

        send_metric(
            project_id,
            cluster_location,
            cluster_name,
            namespace,
            item["metadata"]["name"],
        )


# FIXME:
# * replace with kubernetes client?
# * batch processing?
async def async_request(namespace):
    print(f"==> making request for namespace: {namespace}")
    with open(f"{SERVICEACCOUNT}/token") as f:
        token = f.read()

    url = f"{APISERVER}/api/v1/namespaces/{namespace}/pods"
    _headers = {"Authorization": f"Bearer {token}"}
    cert_path = f"{SERVICEACCOUNT}/ca.crt"

    ssl_context = ssl.create_default_context(cafile=cert_path)
    tcp_connector = aiohttp.TCPConnector(ssl_context=ssl_context)

    async with aiohttp.ClientSession(connector=tcp_connector) as session:
        print(f"=> starting session: {session}")
        async with session.get(url, headers=_headers) as response:
            html = await response.text()

            if response.status != 200:
                # FIXME:
                # * raising generic exceptions is bad..
                raise Exception(f"request failed: {response.status}/{html}")

            return await response.json()


def send_metric(project_id, cluster_location, cluster_name, namespace, pod_name):
    print(
        f"==> updating oom metric for pod: {project_id}/{cluster_location}/{cluster_name}/{namespace}/{pod_name}"
    )
    series = monitoring_v3.TimeSeries()
    series.metric.type = "custom.googleapis.com/gke_oom_kills"
    series.resource.type = "k8s_pod"
    series.resource.labels["project_id"] = project_id
    series.resource.labels["location"] = cluster_location
    series.resource.labels["cluster_name"] = cluster_name
    series.resource.labels["namespace_name"] = namespace
    series.resource.labels["pod_name"] = pod_name

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)

    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )

    point = monitoring_v3.Point({"interval": interval, "value": {"bool_value": True}})

    series.points = [point]

    # FIXME: update to be async
    client = monitoring_v3.MetricServiceClient()

    client.create_time_series(
        request={"name": f"projects/{project_id}", "time_series": [series]}
    )


if __name__ == "__main__":
    asyncio.run(main())
