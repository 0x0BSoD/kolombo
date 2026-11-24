# Kolombo

A terminal UI for exploring a Kubernetes cluster, in the spirit of k9s. Written in Rust with
[kube-rs](https://kube.rs) and [ratatui](https://ratatui.rs).

Kolombo reads your kubeconfig the same way `kubectl` does and uses its current context.

> **Status:** early and under active development. It has only been developed step by step as a
> learning project; expect rough edges. Several actions change the cluster (edit, delete, cordon,
> drain), so try it on a test cluster first.

## Features

- **Dashboard** as the start screen: cluster facts, CPU / memory / pod totals (used and requested),
  ready/total for pods, deployments, statefulsets and daemonsets, and the latest events. `:dash` returns to it.
- **Namespaces and pods**, kept live by watchers. `Enter` on a namespace opens its pods.
- **Any resource type, CRDs included**, through `:` (see below). Pods and namespaces have dedicated
  views; everything else opens in a generic list.
- **Networking and Gateway API** panes (Ingress, IngressClass, NetworkPolicy, GatewayClass, Gateway,
  ListenerSet, HTTPRoute and the other routes, ReferenceGrant, BackendTLSPolicy). `Enter` walks down
  the hierarchy: GatewayClass → Gateway → ListenerSet → HTTPRoute → Service → Pods (`r` on a Gateway
  or ListenerSet jumps straight to its routes). `Esc` climbs back one level at a time.
- **Argo CD** (Application, ApplicationSet, AppProject): `Enter` goes AppProject → ApplicationSets →
  Applications (`r` on a project lists its Applications directly); `Enter` on an Application opens its
  **resource tree** (managed resources, and under workloads the ReplicaSets, Jobs and Pods they own).
- **Helm releases** (`:helm`): read from Helm's release Secrets, newest revision of each. `Enter` shows
  the release's resource tree, `y` its user-supplied values.
- **Jobs and CronJobs** (batch/v1): `Enter` on a CronJob lists its Jobs, on a Job its pods.
- **Nodes** with gauges for CPU, memory and pod count, roles, version, IP and OS image.
  `Enter` shows the pods running on that node.
- **Pod details**: containers with state and last termination, conditions, events, labels and
  annotations. `Enter` on a container shows its full spec (command, args, env sources, ports,
  mounts, probes, resources, security context) and, with metrics-server, CPU and memory graphs.
- **Logs** for one or all containers of a pod, merged by timestamp, with time ranges, autoscroll,
  wrapping and a bare fullscreen mode for copy and paste.
- **YAML pane** with highlighting, folding, vim keys, visual selection and copy.
- **Edit in `$EDITOR`**, delete with confirmation, create namespaces.
- **Cordon, uncordon and drain** (with the usual `kubectl drain` options).
- **Fuzzy search** (`/`), sorting by name, status or age, and a namespace picker.
- **Remembers** the open resource, namespace and sort order between runs.

## Requirements

- A recent stable Rust toolchain (edition 2024) to build from source.
- A reachable cluster and a kubeconfig.
- [metrics-server](https://github.com/kubernetes-sigs/metrics-server) is optional. Without it the
  usage columns and graphs stay empty and the list says `metrics n/a`.
- A system clipboard for copying from the YAML pane (`arboard`).

## Build and run

```sh
git clone git@github.com:0x0BSoD/kolombo.git
cd kolombo
cargo run --release
```

Run the tests with `cargo test`.

## Keys

Press `?` in any view for the keys of that view. The main ones:

| Key | Action |
| --- | --- |
| `j` `k` / arrows | Move |
| `Enter` | Open: namespace → pods, pod → logs, node → its pods |
| `/` | Fuzzy search (`Esc` clears it) |
| `:` | Switch resource type by name or alias (`po`, `svc`, `deploy`, `no`, `ns`, CRDs…) |
| `Ctrl+n` | Namespace picker in the pod list (first row is all namespaces); create namespace in the namespace list |
| `< >` / `A` | Sort column / ascending or descending |
| `d` | Describe |
| `y` | YAML |
| `e` | Edit in `$EDITOR` |
| `Ctrl+d` | Delete (asks first) |
| `l` | Logs of the pod (in a container view: that container) |
| `c` `u` `C` | Node: cordon, uncordon, drain (Shift+c opens the options) |
| `Esc` | Back (namespace list → dashboard, dashboard: quit) |
| `q` | Quit |

Log view: `1` `2` `3` show the last 1, 5 or 15 minutes, `0` shows everything (up to 20000 lines),
`s` toggles autoscroll, `w` toggles wrapping, `f` toggles fullscreen without timestamps or
container names, `h` `l` pan sideways when not wrapping.

YAML view: `Space` folds, `s` shows service fields (managed fields, UID and so on), `v` starts a
selection, `y` copies the selection or line, `Y` copies everything.

## State file

Kolombo keeps its state in `$HOME/.config/kolombo/state` (YAML) and creates it on first run:

```yaml
open: dashboard           # dashboard | namespaces | resource
resource:                 # the open type, as API discovery described it
  name: pods
  kind: Pod
  group: ''
  version: v1
  namespaced: true
namespace: kube-system    # omitted = all namespaces
sort:
  namespaces: { column: name, ascending: true }
  resource:   { column: age,  ascending: false }
```

Missing fields fall back to defaults and a damaged file is ignored, so it is safe to edit or delete.

## Things to know

- Pod edits often fail with `422` (immutable fields) or `409` (the kubelet changed the object);
  that is the API server, not a bug in the editor round trip.
- Drain deletes pods. `Force` together with `Disable eviction` bypasses every safety check.
- A node's pod count needs a watch on every pod in the cluster, which is heavy on very large
  clusters. It only runs while the node list is open.
- The generic list shows only columns every object has (name, a guessed status, age, labels), not
  the per-type printer columns that `kubectl get` shows.
- Without RBAC permission to list a resource, the list stays empty and the status line shows the
  watch error.

## License

[MIT](LICENSE)
