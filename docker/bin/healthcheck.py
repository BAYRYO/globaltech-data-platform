#!/usr/bin/env python3
import json
import socket
import sys
import urllib.request


def get(url: str):
    with urllib.request.urlopen(url, timeout=4) as resp:
        return json.load(resp)


def jmx(port: int, bean: str) -> dict:
    return get(f"http://localhost:{port}/jmx?qry={bean}")["beans"][0]


def namenode() -> bool:
    return jmx(9870, "Hadoop:service=NameNode,name=NameNodeStatus")["State"] == "active"


def datanode() -> bool:
    info = jmx(9864, "Hadoop:service=DataNode,name=DataNodeInfo")
    return any(a["ActorState"] == "RUNNING" for a in json.loads(info["BPServiceActorInfo"]))


def resourcemanager() -> bool:
    return get("http://localhost:8088/ws/v1/cluster/info")["clusterInfo"]["state"] == "STARTED"


def nodemanager() -> bool:
    nodes = (get("http://resourcemanager:8088/ws/v1/cluster/nodes")["nodes"] or {}).get("node", [])
    host = socket.gethostname()
    return any(n["nodeHostName"] == host and n["state"] == "RUNNING" for n in nodes)


def spark_history() -> bool:
    get("http://localhost:18080/api/v1/applications?limit=1")
    return True


def mr_history() -> bool:
    return "historyInfo" in get("http://localhost:19888/ws/v1/history/info")


CHECKS = {
    "namenode": namenode,
    "datanode": datanode,
    "resourcemanager": resourcemanager,
    "nodemanager": nodemanager,
    "spark-history": spark_history,
    "mr-history": mr_history,
}

if __name__ == "__main__":
    try:
        healthy = CHECKS[sys.argv[1]]()
    except Exception as err:
        print(f"{type(err).__name__}: {err}")
        healthy = False
    sys.exit(0 if healthy else 1)
