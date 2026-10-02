"""Kubernetes side of the local controller: scale deployments, manage HPAs, read pod state
and CPU usage (metrics.k8s.io, provided by metrics-server; k3s ships it by default)."""

from kubernetes import client, config
from kubernetes.client.rest import ApiException


def parse_cpu(quantity):
    """Kubernetes CPU quantity ("250m", "1", "123456789n", "15u") -> cores."""
    if quantity is None:
        return 0.0
    quantity = str(quantity)
    units = {"n": 1e-9, "u": 1e-6, "m": 1e-3}
    if quantity[-1] in units:
        return float(quantity[:-1]) * units[quantity[-1]]
    return float(quantity)


class KubeBackend:

    def __init__(self, namespace, stage_cfg):
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.namespace = namespace
        self.stage_cfg = stage_cfg
        self.apps = client.AppsV1Api()
        self.core = client.CoreV1Api()
        self.autoscaling = client.AutoscalingV2Api()
        self.custom = client.CustomObjectsApi()

    def _deployment(self, stage):
        return self.stage_cfg[stage]["deployment"]

    def _selector(self, stage):
        return f"app={self.stage_cfg[stage]['app_label']}"

    def _hpa_name(self, stage):
        return f"{stage}-hpa"

    # Actions

    def scale(self, stage, replicas):
        self.apps.patch_namespaced_deployment_scale(
            self._deployment(stage), self.namespace, {"spec": {"replicas": replicas}})

    def ensure_hpa(self, stage, hpa):
        body = {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": self._hpa_name(stage)},
            "spec": {
                "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment",
                                   "name": self._deployment(stage)},
                "minReplicas": hpa["min"],
                "maxReplicas": hpa["max"],
                "metrics": [{"type": "Resource", "resource": {
                    "name": "cpu", "target": {"type": "Utilization",
                                              "averageUtilization": hpa["cpu_target"]}}}],
            },
        }
        if "behavior" in hpa:
            body["spec"]["behavior"] = hpa["behavior"]
        try:
            self.autoscaling.create_namespaced_horizontal_pod_autoscaler(self.namespace, body)
        except ApiException as exc:
            if exc.status != 409:  # 409: already exists
                raise

    def delete_hpa(self, stage):
        try:
            self.autoscaling.delete_namespaced_horizontal_pod_autoscaler(
                self._hpa_name(stage), self.namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise

    # State

    def desired(self, stage):
        dep = self.apps.read_namespaced_deployment(self._deployment(stage), self.namespace)
        return dep.spec.replicas or 0

    def ready(self, stage):
        dep = self.apps.read_namespaced_deployment(self._deployment(stage), self.namespace)
        return dep.status.ready_replicas or 0

    def snapshot(self):
        """stage -> desired, ready, alive (pods that exist and are not finished, including
        the ones shutting down), terminating (deletion requested, still draining), hpa."""
        deployments = {d.metadata.name: d for d in
                       self.apps.list_namespaced_deployment(self.namespace).items}
        pods = self.core.list_namespaced_pod(self.namespace).items
        hpas = {h.metadata.name for h in
                self.autoscaling.list_namespaced_horizontal_pod_autoscaler(self.namespace).items}
        result = {}
        for stage, cfg in self.stage_cfg.items():
            dep = deployments.get(cfg["deployment"])
            stage_pods = [p for p in pods
                          if (p.metadata.labels or {}).get("app") == cfg["app_label"]
                          and p.status.phase not in ("Succeeded", "Failed")]
            result[stage] = {
                "desired": (dep.spec.replicas or 0) if dep else 0,
                "ready": (dep.status.ready_replicas or 0) if dep else 0,
                "alive": len(stage_pods),
                "terminating": sum(1 for p in stage_pods if p.metadata.deletion_timestamp),
                "hpa": self._hpa_name(stage) in hpas,
            }
        return result

    def cpu_per_stage(self):
        """stage -> mean over its pods of (CPU usage / CPU limit). Missing if no metrics yet."""
        try:
            metrics = self.custom.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", self.namespace, "pods")["items"]
        except ApiException:
            return {}
        pods = {p.metadata.name: p for p in self.core.list_namespaced_pod(self.namespace).items}
        per_stage = {}
        for item in metrics:
            pod = pods.get(item["metadata"]["name"])
            if pod is None:
                continue
            app = (pod.metadata.labels or {}).get("app")
            stage = next((s for s, c in self.stage_cfg.items() if c["app_label"] == app), None)
            if stage is None:
                continue
            usage = sum(parse_cpu(c["usage"]["cpu"]) for c in item["containers"])
            limit = sum(parse_cpu((c.resources.limits or {}).get("cpu")) for c in pod.spec.containers)
            if limit > 0:
                per_stage.setdefault(stage, []).append(usage / limit)
        return {s: sum(v) / len(v) for s, v in per_stage.items()}

    def site_utilization(self):
        """CPU utilization of the site's nodes and of the pipeline pods.

        `pipeline` (usage of the stage pods / sum of their CPU limits) is the per-layer
        utilization fed to the master agent; `nodes` is the whole machine.
        """
        result = {}
        try:
            node_metrics = self.custom.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "nodes")["items"]
            nodes = {n.metadata.name: n for n in self.core.list_node().items}
            used = sum(parse_cpu(m["usage"]["cpu"]) for m in node_metrics)
            capacity = sum(parse_cpu(nodes[m["metadata"]["name"]].status.allocatable["cpu"])
                           for m in node_metrics if m["metadata"]["name"] in nodes)
            result["nodes"] = used / capacity if capacity else None
        except ApiException:
            result["nodes"] = None
        try:
            metrics = self.custom.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", self.namespace, "pods")["items"]
            pods = {p.metadata.name: p for p in self.core.list_namespaced_pod(self.namespace).items}
            labels = {c["app_label"] for c in self.stage_cfg.values()}
            used = limit = 0.0
            for item in metrics:
                pod = pods.get(item["metadata"]["name"])
                if pod is None or (pod.metadata.labels or {}).get("app") not in labels:
                    continue
                used += sum(parse_cpu(c["usage"]["cpu"]) for c in item["containers"])
                limit += sum(parse_cpu((c.resources.limits or {}).get("cpu")) for c in pod.spec.containers)
            result["pipeline"] = used / limit if limit else 0.0
        except ApiException:
            result["pipeline"] = None
        return result
