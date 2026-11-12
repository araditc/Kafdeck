# KAF-02 deployment NetworkPolicy retest

The deployment test passed on `8f4f8b228f91ef6b51e9d679b09a47463a40154c`. An isolated, single-node K3s `v1.34.13+k3s1` cluster ran in a dedicated Linux network namespace in disposable Ubuntu WSL 2. K3s used flannel `host-gw` and the embedded kube-router `v2.11.1-k3s2` NetworkPolicy controller. The rendered Kafdeck chart deployed its actual Deployment, Service, ServiceAccount and NetworkPolicy. The candidate image was loaded from the verified main archive. Containerd resolved the manifest's config digest to the candidate provenance image ID. [K3s network policy controller](https://docs.k3s.io/networking/networking-services#network-policy-controller) · [Candidate source](https://github.com/araditc/Kafdeck/tree/8f4f8b228f91ef6b51e9d679b09a47463a40154c)

- Candidate archive SHA256 `0ae4046314e33e1763e30db0a4a1e1eb859dfd0494623e75d8180e3ec0b64528`
- Loaded image manifest `sha256:0e60fd15beea160d8323a7f57c4f3728ee1af222b17ce755dd6c6746a01748e2`
- Candidate config `sha256:04ce96805b379ca19e8e1846ad5acb1c2fc60ec6c08a8d91770a274838bae225`

An Apache Kafka 4.3.1 fixture in a separate routed namespace initially advertised `172.30.71.20:9092`. The broker restarted with its original data and a new advertised IP of `172.30.72.20`, outside the approved `172.30.71.20/32`. The TLS client used Confluent.Kafka 2.16.0 with certificate and endpoint verification enabled. The probe ran as UID 1654 inside the actual chart container. The fixture used containerd task exec after the nested cluster's kubelet streaming proxy returned HTTP 502.

| Scenario | Observed result |
| --- | --- |
| Initial broker metadata | TLS-verified metadata returned the approved address. |
| Positive control with both CIDRs allowed | The changed advertised address received 2 TCP SYN packets. Kafka acknowledged a persisted record at offset `0`. |
| Shipped chart policy rendered with only the approved CIDR | Kubernetes applied the restricted NetworkPolicy to the same Pod UID. The approved address still returned TLS-verified metadata showing the changed advertised IP. Kube-router installed rules for the Pod IP. |
| Produce to the newly advertised outside address | The broker's outside interface received 0 SYN packets. The producer returned `Local_MsgTimedOut` with `NotPersisted`. The broker end offset was `1` before and after this attempt. |

The deployment runner exited `0`. An earlier fixture run exposed a blank shutdown line in `tcpdump` output. The final run counted packet records and retained broker offsets directly. Scope is the tested Kubernetes/CNI, chart policy, image, and direct IP route to a broker outside the cluster. Service DNAT and any other production topology need their own deployment checks. The standalone producer exercised the chart Pod's egress boundary. The application's registration and authorization paths have separate evidence. [Kubernetes ipBlock semantics](https://kubernetes.io/docs/concepts/services-networking/network-policies/#behavior-of-to-and-from-selectors)

| Retained evidence | SHA256 |
| --- | --- |
| [Machine-readable result](observed-final/kaf02-deployment-result.json) | `dc07ba6a6114b570b4a222a87115309586cf4e519a9c821d79c5cee4fdd02220` |
| [Applied restricted NetworkPolicy](observed-final/restricted-policy.json) | `8428761f6bef068b5364b9d45dde5bcbdf7c5d609d2b1397c33a18d2648af0ff` |
| [Positive SYN capture](observed-final/positive-syn.txt) | `e5f11e214227b632ba53613ced5f424184a89407230865714d1d49ce7b0e3f7a` |
| [Restricted SYN capture](observed-final/restricted-syn.txt) | `01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b` |
| [Kube-router pod rules](observed-final/cni-pod-rules.txt) | `1555b3b8cec984bdc1cd4a74d297296017b8d68837c78f0d9e5fae1581605f53` |
| [K3s NetworkPolicy controller log](observed-final/k3s-network-controller.txt) | `85258cbe85d14ad5e89b0a78a83f9299e360934a60aacd4de38bbaa6fc55cbe5` |

The fixture harness and portable TLS probe sources are [run-deployment.sh](run-deployment.sh) and [Program.cs](Program.cs). The control-plane harness is [start-k3s.sh](start-k3s.sh).

The runner removed its Kubernetes namespace and volume. After verifying the lab's WSL registration name and storage path, the isolated distribution was terminated and unregistered. Workspace evidence was retained. [Cleanup record](observed-final/lab-cleanup.txt)
