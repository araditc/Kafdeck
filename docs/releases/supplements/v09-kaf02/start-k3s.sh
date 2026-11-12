#!/usr/bin/env bash
set -euo pipefail

[[ ${WSL_DISTRO_NAME:-} == Kafdeck-KAF02-Deploy-8f4f8b2 && $(id -u) == 0 ]]
assets=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lab=/opt/kafdeck-cni-lab
install -d -m 0700 "$lab"
printf '%s  %s\n' b9a7c47aab81db66d0253378ddea0d06f8a6dca4dace7a26db3f00fa2f0e3717 "$assets/k3s" | sha256sum -c -
install -m 0755 "$assets/k3s" "$lab/k3s"
install -d -m 0700 "$lab/data/agent/images"
install -m 0600 "$lab/pause-image.tar" "$lab/data/agent/images/pause-image.tar"

export LAB="$lab" OUTER_NETNS="$(readlink /proc/self/ns/net)"
exec unshare --net -- bash -c '
  set -euo pipefail
  [[ $(readlink /proc/self/ns/net) != "$OUTER_NETNS" ]]
  printf "%s\n" "$$" >"$LAB/node.pid"
  ip link set lo up
  ip link add kafnode type bridge
  ip addr add 172.31.251.1/24 dev kafnode
  ip link set kafnode up
  ip route add default dev kafnode
  printf 1 >/proc/sys/net/ipv4/ip_forward
  ipset create kaf02-preflight hash:ip
  ipset destroy kaf02-preflight
  exec "$LAB/k3s" server --data-dir "$LAB/data" --write-kubeconfig "$LAB/kubeconfig" \
    --write-kubeconfig-mode 600 --node-name kaf02-fixture --bind-address 127.0.0.1 \
    --advertise-address 172.31.251.1 --node-ip 172.31.251.1 --flannel-iface kafnode \
    --flannel-backend host-gw --cluster-cidr 10.197.0.0/16 --service-cidr 10.198.0.0/16 \
    --cluster-dns 10.198.0.10 --disable coredns,traefik,servicelb,metrics-server,local-storage \
    --pause-image registry.k8s.io/pause:3.10.1 --kubelet-arg cgroup-driver=cgroupfs \
    --kubelet-arg address=172.31.251.1 --kubelet-arg fail-swap-on=false \
    >"$LAB/k3s.log" 2>&1
'
