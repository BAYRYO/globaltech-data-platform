#!/usr/bin/env bash
# Show service health, live and dead DataNodes, NodeManagers and the web UI addresses.
# Usage: scripts/cluster/status.sh
# shellcheck source=scripts/cluster/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

[[ -n "$(compose ps --quiet --status running namenode)" ]] ||
  die "the cluster is not running: scripts/cluster/up.sh"

compose ps --format 'table {{.Service}}\t{{.Status}}'

compose exec -T namenode python3 - <<'EOF'
import json
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.load(resp)


nn = get("http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")["beans"][0]
live, dead = json.loads(nn["LiveNodes"]), json.loads(nn["DeadNodes"])
print(f"\nHDFS  {len(live)} live / {len(dead)} dead DataNodes, safe mode {'ON' if nn['Safemode'] else 'off'}")
for name, node in sorted(live.items()):
    print(f"  {name:<24} live   {node['numBlocks']:>6} blocks  {node['usedSpace'] / 2**20:>9,.0f} MiB used")
for name in sorted(dead):
    print(f"  {name:<24} DEAD")

nodes = (get("http://resourcemanager:8088/ws/v1/cluster/nodes")["nodes"] or {}).get("node", [])
running = sum(n["state"] == "RUNNING" for n in nodes)
print(f"\nYARN  {running} RUNNING / {len(nodes)} NodeManagers")
for n in sorted(nodes, key=lambda n: n["id"]):
    total = n["usedMemoryMB"] + n["availMemoryMB"]
    print(f"  {n['id']:<24} {n['state']:<8} {n['numContainers']:>3} containers  {n['usedMemoryMB']:>5}/{total} MB")
EOF

cat <<'EOF'

Web UIs
  NameNode           http://localhost:9870
  ResourceManager    http://localhost:8088
  Spark History      http://localhost:18080
  MapReduce History  http://localhost:19888
EOF
