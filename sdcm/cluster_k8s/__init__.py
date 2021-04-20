# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

# pylint: disable=too-many-arguments

from __future__ import annotations

import contextlib
import logging
import os
import random as librandom
import requests
import time
import base64
import zipfile

from copy import deepcopy
from datetime import datetime
from difflib import unified_diff
from functools import cached_property, partialmethod, partial
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from threading import RLock
from typing import Optional, Union, List, Dict, Any, ContextManager, Type, Callable

import json
import yaml
import kubernetes as k8s
from kubernetes.client import V1Container
from kubernetes.dynamic.resource import Resource, ResourceField, ResourceInstance, ResourceList, Subresource

from invoke.exceptions import CommandTimedOut

from sdcm import sct_abs_path, cluster, cluster_docker
from sdcm.db_stats import PrometheusDBStats
from sdcm.remote import LOCALRUNNER, NETWORK_EXCEPTIONS
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.coredump import CoredumpExportFileThread
from sdcm.mgmt import AnyManagerCluster
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.common import download_from_github, walk_thru_data
from sdcm.utils.k8s import KubernetesOps, ApiCallRateLimiter, JSON_PATCH_TYPE, KUBECTL_TIMEOUT
from sdcm.utils.decorators import log_run_info, retrying, timeout
from sdcm.utils.remote_logger import get_system_logging_thread, CertManagerLogger, ScyllaOperatorLogger, \
    KubectlClusterEventsLogger, ScyllaManagerLogger
from sdcm.utils.version_utils import get_git_tag_from_helm_chart_version
from sdcm.wait import wait_for
from sdcm.cluster_k8s.operator_monitoring import ScyllaOperatorLogMonitoring, ScyllaOperatorStatusMonitoring


ANY_KUBERNETES_RESOURCE = Union[Resource, ResourceField, ResourceInstance, ResourceList, Subresource]

CERT_MANAGER_TEST_CONFIG = sct_abs_path("sdcm/k8s_configs/cert-manager-test.yaml")
SCYLLA_API_VERSION = "scylla.scylladb.com/v1"
SCYLLA_CLUSTER_RESOURCE_KIND = "ScyllaCluster"
DEPLOY_SCYLLA_CLUSTER_DELAY = 15  # seconds
SCYLLA_OPERATOR_NAMESPACE = "scylla-operator-system"
SCYLLA_MANAGER_NAMESPACE = "scylla-manager-system"
SCYLLA_NAMESPACE = "scylla"


LOGGER = logging.getLogger(__name__)


class KubernetesCluster:
    api_call_rate_limiter: Optional[ApiCallRateLimiter] = None

    datacenter = ()
    _cert_manager_journal_thread: Optional[CertManagerLogger] = None
    _scylla_manager_journal_thread: Optional[ScyllaManagerLogger] = None
    _scylla_operator_journal_thread: Optional[ScyllaOperatorLogger] = None
    _scylla_cluster_events_thread: Optional[KubectlClusterEventsLogger] = None

    _scylla_operator_log_monitor_thread: Optional[ScyllaOperatorLogMonitoring] = None
    _scylla_operator_status_monitor_thread: Optional[ScyllaOperatorStatusMonitoring] = None

    # NOTE: Following class attr(s) are defined for consumers of this class
    #       such as 'sdcm.utils.remote_logger.ScyllaOperatorLogger'.
    _scylla_operator_namespace = SCYLLA_OPERATOR_NAMESPACE
    _scylla_manager_namespace = SCYLLA_MANAGER_NAMESPACE
    _scylla_namespace = SCYLLA_NAMESPACE

    @property
    def k8s_server_url(self) -> Optional[str]:
        return None

    kubectl_cmd = partialmethod(KubernetesOps.kubectl_cmd)
    apply_file = partialmethod(KubernetesOps.apply_file)

    def kubectl_no_wait(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                        verbose=True):
        return KubernetesOps.kubectl(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                     ignore_status=ignore_status, verbose=verbose)

    def kubectl(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                verbose=True):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return KubernetesOps.kubectl(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                     ignore_status=ignore_status, verbose=verbose)

    def kubectl_multi_cmd(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                          verbose=True):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return KubernetesOps.kubectl_multi_cmd(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                               ignore_status=ignore_status, verbose=verbose)

    @cached_property
    def helm(self):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return partial(cluster.Setup.tester_obj().localhost.helm, self)

    @cached_property
    def helm_install(self):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return partial(cluster.Setup.tester_obj().localhost.helm_install, self)

    @property
    def cert_manager_log(self) -> str:
        return os.path.join(self.logdir, "cert_manager.log")

    @property
    def scylla_manager_log(self) -> str:
        return os.path.join(self.logdir, "scylla_manager.log")

    def start_cert_manager_journal_thread(self) -> None:
        self._cert_manager_journal_thread = CertManagerLogger(self, self.cert_manager_log)
        self._cert_manager_journal_thread.start()

    def start_scylla_manager_journal_thread(self):
        self._scylla_manager_journal_thread = ScyllaManagerLogger(self, self.scylla_manager_log)
        self._scylla_manager_journal_thread.start()

    @log_run_info
    def deploy_cert_manager(self) -> None:
        LOGGER.info("Deploy cert-manager")
        self.kubectl("create namespace cert-manager", ignore_status=True)
        LOGGER.debug(self.helm("repo add jetstack https://charts.jetstack.io"))
        LOGGER.debug(self.helm(f"install cert-manager jetstack/cert-manager "
                               f"--version v{self.params.get('k8s_cert_manager_version')} --set installCRDs=true",
                               namespace="cert-manager"))
        time.sleep(10)
        self.kubectl("wait --timeout=10m --all --for=condition=Ready pod", namespace="cert-manager")
        wait_for(
            self.check_if_cert_manager_fully_functional,
            text='Waiting for cert-manager to become fully operational',
            timeout=10 * 60,
            step=10,
            throw_exc=True)
        self.start_cert_manager_journal_thread()

    @cached_property
    def _scylla_operator_chart_version(self):
        LOGGER.debug(self.helm(
            f"repo add scylla-operator {self.params.get('k8s_scylla_operator_helm_repo')}"))

        # NOTE: 'scylla-operator' and 'scylla-manager' chart versions are always the same.
        #       So, we can reuse one for another.
        chart_version = self.params.get(
            "k8s_scylla_operator_chart_version").strip().lower()
        if chart_version in ("", "latest"):
            latest_version_raw = self.helm(
                "search repo scylla-operator/scylla-operator --devel -o yaml")
            latest_version = yaml.safe_load(latest_version_raw)
            assert isinstance(
                latest_version, list), f"Expected list of data, got: {type(latest_version)}"
            assert len(latest_version) == 1, "Expected only one element in the list of versions"
            assert "version" in latest_version[0], "Expected presence of 'version' key"
            chart_version = latest_version[0]["version"].strip()
            LOGGER.info(f"Using automatically found following "
                        f"latest scylla-operator chart version: {chart_version}")
        else:
            LOGGER.info(f"Using following predefined scylla-operator "
                        f"chart version: {chart_version}")
        return chart_version

    @log_run_info
    def deploy_scylla_manager(self) -> None:
        # Calculate options values which must be set
        #
        # image.tag                  -> self.params.get('mgmt_docker_image').split(':')[-1]
        # controllerImage.repository -> self.params.get(
        #                                   'k8s_scylla_operator_docker_image').split('/')[0]
        # controllerImage.tag        -> self.params.get(
        #                                   'k8s_scylla_operator_docker_image').split(':')[-1]
        set_options = []

        mgmt_docker_image_tag = self.params.get('mgmt_docker_image').split(':')[-1]
        if mgmt_docker_image_tag:
            set_options.append(f"image.tag={mgmt_docker_image_tag}")

        scylla_operator_repo_base = self.params.get(
            'k8s_scylla_operator_docker_image').split('/')[0]
        if scylla_operator_repo_base:
            set_options.append(f"controllerImage.repository={scylla_operator_repo_base}")

        scylla_operator_image_tag = self.params.get(
            'k8s_scylla_operator_docker_image').split(':')[-1]
        if scylla_operator_image_tag:
            set_options.append(f"controllerImage.tag={scylla_operator_image_tag}")

        # Install and wait for initialization of the Scylla Manager chart
        LOGGER.info("Deploy scylla-manager")
        LOGGER.debug(self.helm_install(
            target_chart_name="scylla-manager",
            source_chart_name="scylla-operator/scylla-manager",
            version=self._scylla_operator_chart_version,
            use_devel=True,
            set_options=",".join(set_options),
            namespace=SCYLLA_MANAGER_NAMESPACE,
        ))
        time.sleep(10)
        self.kubectl("wait --timeout=10m --all --for=condition=Ready pod",
                     namespace=SCYLLA_MANAGER_NAMESPACE)

        # Start the Scylla Manager logging thread
        self.start_scylla_manager_journal_thread()

    def check_if_cert_manager_fully_functional(self) -> bool:
        # Cert-manager readiness status does not guarantee that it is fully operational
        # This function checks it if is operational via deploying ca and issuing certificate
        try:
            self.apply_file(CERT_MANAGER_TEST_CONFIG)
            return True
        finally:
            self.kubectl(f'delete -f {CERT_MANAGER_TEST_CONFIG}', ignore_status=True)

    @property
    def scylla_operator_log(self) -> str:
        return os.path.join(self.logdir, "scylla_operator.log")

    @property
    def scylla_cluster_event_log(self) -> str:
        return os.path.join(self.logdir, "scylla_cluster_events.log")

    def start_scylla_operator_journal_thread(self) -> None:
        self._scylla_operator_journal_thread = ScyllaOperatorLogger(self, self.scylla_operator_log)
        self._scylla_operator_journal_thread.start()
        self._scylla_operator_log_monitor_thread = ScyllaOperatorLogMonitoring(self)
        self._scylla_operator_log_monitor_thread.start()
        self._scylla_operator_status_monitor_thread = ScyllaOperatorStatusMonitoring(self)
        self._scylla_operator_status_monitor_thread.start()

    def start_scylla_cluster_events_thread(self) -> None:
        self._scylla_cluster_events_thread = KubectlClusterEventsLogger(self, self.scylla_cluster_event_log)
        self._scylla_cluster_events_thread.start()

    @log_run_info
    def deploy_scylla_operator(self) -> None:
        # Calculate options values which must be set
        #
        # image.repository -> self.params.get('k8s_scylla_operator_docker_image').split('/')[0]
        # image.tag        -> self.params.get('k8s_scylla_operator_docker_image').split(':')[-1]
        set_options = []

        scylla_operator_repo_base = self.params.get(
            'k8s_scylla_operator_docker_image').split('/')[0]
        if scylla_operator_repo_base:
            set_options.append(f"controllerImage.repository={scylla_operator_repo_base}")

        scylla_operator_image_tag = self.params.get(
            'k8s_scylla_operator_docker_image').split(':')[-1]
        if scylla_operator_image_tag:
            set_options.append(f"controllerImage.tag={scylla_operator_image_tag}")

        # Install and wait for initialization of the Scylla Operator chart
        LOGGER.info("Deploy Scylla Operator")
        self.kubectl(f'create namespace {SCYLLA_OPERATOR_NAMESPACE}')
        LOGGER.debug(self.helm_install(
            target_chart_name="scylla-operator",
            source_chart_name="scylla-operator/scylla-operator",
            version=self._scylla_operator_chart_version,
            use_devel=True,
            set_options=",".join(set_options),
            namespace=SCYLLA_OPERATOR_NAMESPACE,
        ))
        time.sleep(10)
        self.kubectl("wait --timeout=5m --all --for=condition=Ready pod",
                     namespace=SCYLLA_OPERATOR_NAMESPACE)

        # Start the Scylla Operator logging thread
        self.start_scylla_operator_journal_thread()

    @log_run_info
    def deploy_minio_s3_backend(self, minio_bucket_name):
        LOGGER.info('Deploy minio s3-like backend server')
        self.helm('repo add minio https://helm.min.io/')
        self.kubectl("create namespace minio")
        self.helm(
            'install --set accessKey=minio_access_key,secretKey=minio_access_key,'
            f'defaultBucket.enabled=true,defaultBucket.name={minio_bucket_name},'
            'defaultBucket.policy=public --generate-name minio/minio',
            namespace='minio')

        minio_ip_address = wait_for(
            lambda: self.minio_ip_address, text='Waiting for minio pod to popup', timeout=120, throw_exc=True)

        self.kubectl("wait --timeout=10m --all --for=condition=Ready pod", namespace="minio")

    @log_run_info
    def deploy_scylla_cluster(self, config: str, target_mgmt_agent_to_minio: bool = False) -> None:
        LOGGER.info("Create and initialize a Scylla cluster")
        if target_mgmt_agent_to_minio:
            # Create kubernetes secret that holds scylla manager agent configuration
            self.update_secret_from_data('scylla-agent-config', 'scylla', {
                'scylla-manager-agent.yaml': {
                    's3': {
                        'provider': 'Minio',
                        'endpoint': f"http://{self.minio_ip_address}:9000",
                        'access_key_id': 'minio_access_key',
                        'secret_access_key': 'minio_access_key'
                    }
                }
            })

        # Deploy scylla cluster
        LOGGER.debug(self.helm_install(
            target_chart_name="scylla",
            source_chart_name="scylla-operator/scylla",
            version=self._scylla_operator_chart_version,
            use_devel=True,
            set_options="",
            values_file_path=config,
            namespace=SCYLLA_NAMESPACE,
        ))

        LOGGER.debug("Check Scylla cluster")
        self.kubectl("get scyllaclusters.scylla.scylladb.com", namespace=SCYLLA_NAMESPACE)
        self.kubectl("get pods", namespace=SCYLLA_NAMESPACE)

        LOGGER.debug("Wait for %d secs before we start to apply changes to the cluster", DEPLOY_SCYLLA_CLUSTER_DELAY)
        time.sleep(DEPLOY_SCYLLA_CLUSTER_DELAY)
        self.start_scylla_cluster_events_thread()

    @log_run_info
    def deploy_loaders_cluster(self, config: str) -> None:
        LOGGER.info("Create and initialize a loaders cluster")
        self.apply_file(config)

        LOGGER.debug("Check the loaders cluster")
        self.kubectl("get statefulset", namespace="sct-loaders")
        self.kubectl("get pods", namespace="sct-loaders")

    @log_run_info
    def deploy_monitoring_cluster(
            self, scylla_operator_tag: str, namespace: str = "monitoring", is_manager_deployed: bool = False) -> None:
        """
        This procedure comes from scylla-operator repo:
        https://github.com/scylladb/scylla-operator/blob/master/docs/source/generic.md#setting-up-monitoring

        If it fails please consider reporting and fixing issue in scylla-operator repo too
        """
        LOGGER.info("Create and initialize a monitoring cluster")
        if scylla_operator_tag == 'nightly':
            scylla_operator_tag = 'master'
        elif scylla_operator_tag == '':
            scylla_operator_tag = get_git_tag_from_helm_chart_version(
                self._scylla_operator_chart_version)
        with TemporaryDirectory() as tmp_dir_name:
            scylla_operator_dir = os.path.join(tmp_dir_name, 'scylla-operator')
            scylla_monitoring_dir = os.path.join(tmp_dir_name, 'scylla-monitoring')
            download_from_github(
                repo='scylladb/scylla-operator',
                tag=scylla_operator_tag,
                dst_dir=scylla_operator_dir)
            download_from_github(
                repo='scylladb/scylla-monitoring',
                tag='scylla-monitoring-3.6.0',
                dst_dir=scylla_monitoring_dir)
            self.kubectl(f'create namespace {namespace}')
            self.helm('repo add prometheus-community https://prometheus-community.github.io/helm-charts')
            self.helm('repo update')
            self.helm(
                f'install monitoring prometheus-community/kube-prometheus-stack '
                f'--values {os.path.join(scylla_operator_dir, "examples", "common", "monitoring", "values.yaml")} '
                '--set server.resources.limits.cpu=2 --set server.resources.limits.memory=4Gi'
                f'--create-namespace --namespace {namespace}')

            self.apply_file(os.path.join(scylla_operator_dir, "examples", "common", "monitoring",
                                         "scylla-service-monitor.yaml"))
            self.kubectl(
                f'create configmap scylla-dashboards --from-file={scylla_monitoring_dir}/grafana/build/ver_4.3',
                namespace=namespace)
            self.kubectl(
                "patch configmap scylla-dashboards -p '{\"metadata\":{\"labels\":{\"grafana_dashboard\": \"1\"}}}'",
                namespace=namespace)

            if is_manager_deployed:
                self.apply_file(os.path.join(scylla_operator_dir, "examples", "common", "monitoring",
                                             "scylla-manager-service-monitor.yaml"))
                self.kubectl(
                    f'create configmap scylla-manager-dashboards '
                    f'--from-file={scylla_monitoring_dir}/grafana/build/manager_2.2',
                    namespace=namespace)
                self.kubectl(
                    "patch configmap scylla-manager-dashboards "
                    "-p '{\"metadata\":{\"labels\":{\"grafana_dashboard\": \"1\"}}}'",
                    namespace=namespace)

        time.sleep(10)
        self.kubectl("wait --timeout=15m --all --for=condition=Ready pod", timeout=1000, namespace=namespace)
        LOGGER.debug("Check the monitoring cluster")
        self.kubectl("get statefulset", namespace=namespace)
        self.kubectl("get pods", namespace=namespace)

    @log_run_info
    def stop_k8s_task_threads(self, timeout=10):
        LOGGER.info("Stop k8s task threads")
        if self._scylla_manager_journal_thread:
            self._scylla_manager_journal_thread.stop(timeout)
        if self._cert_manager_journal_thread:
            self._cert_manager_journal_thread.stop(timeout)
        if self._scylla_operator_log_monitor_thread:
            self._scylla_operator_log_monitor_thread.stop()
        if self._scylla_operator_status_monitor_thread:
            self._scylla_operator_status_monitor_thread.stop()
        if self._scylla_operator_journal_thread:
            self._scylla_operator_journal_thread.stop(timeout)
        if self._scylla_cluster_events_thread:
            self._scylla_cluster_events_thread.stop(timeout)

    @property
    def minio_pod(self) -> Resource:
        for pod in KubernetesOps.list_pods(self, namespace='minio'):
            found = False
            for container in pod.spec.containers:
                if any([portRec for portRec in container.ports
                        if hasattr(portRec, 'container_port') and str(portRec.container_port) == '9000']):
                    found = True
                    break
            if found:
                return pod
        raise RuntimeError("Can't find minio pod")

    @property
    def minio_ip_address(self) -> str:
        return self.minio_pod.status.pod_ip

    @property
    def operator_pod_status(self):
        pods = KubernetesOps.list_pods(self, namespace=SCYLLA_OPERATOR_NAMESPACE)
        return pods[0].status if pods else None

    @property
    def k8s_core_v1_api(self) -> k8s.client.CoreV1Api:
        return KubernetesOps.core_v1_api(self.api_client)

    @property
    def k8s_apps_v1_api(self) -> k8s.client.AppsV1Api:
        return KubernetesOps.apps_v1_api(self.api_client)

    @property
    def k8s_configuration(self) -> k8s.client.Configuration:
        if self.api_call_rate_limiter:
            return self.api_call_rate_limiter.get_k8s_configuration(self)
        return KubernetesOps.create_k8s_configuration(self)

    @property
    def api_client(self) -> k8s.client.ApiClient:
        return self.get_api_client()

    def get_api_client(self) -> k8s.client.ApiClient:
        if self.api_call_rate_limiter:
            return self.api_call_rate_limiter.get_api_client(self.k8s_configuration)
        return KubernetesOps.api_client(self.k8s_configuration)

    @property
    def dynamic_client(self) -> k8s.dynamic.DynamicClient:
        return KubernetesOps.dynamic_client(self.api_client)

    @cached_property
    def scylla_manager_cluster(self) -> 'PodCluster':
        return PodCluster(
            k8s_cluster=self,
            namespace=SCYLLA_MANAGER_NAMESPACE,
            container='scylla-manager',
            cluster_prefix='mgr-',
            node_prefix='mgr-node-',
            n_nodes=1
        )

    def create_secret_from_data(self, secret_name: str, namespace: str, data: dict, secret_type: str = 'Opaque'):
        prepared_data = {key: base64.b64encode(json.dumps(value).encode('utf-8')).decode('utf-8')
                         for key, value in data.items()}
        secret = k8s.client.V1Secret(
            'v1',
            prepared_data,
            'Secret',
            {'name': secret_name, 'namespace': namespace},
            type=secret_type)
        self.k8s_core_v1_api.create_namespaced_secret(namespace, secret)

    def update_secret_from_data(self, secret_name: str, namespace: str, data: dict, secret_type: str = 'Opaque'):
        existing = None
        for secret in self.k8s_core_v1_api.list_namespaced_secret(namespace).items:
            if secret.metadata.name == secret_name:
                existing = secret
                break
        if not existing:
            self.create_secret_from_data(
                secret_name=secret_name, namespace=namespace, data=data, secret_type=secret_type)
            return

        for key, value in data.items():
            existing.data[key] = base64.b64encode(json.dumps(value).encode('utf-8')).decode('utf-8')
        self.k8s_core_v1_api.patch_namespaced_secret(secret_name, namespace, existing)

    def create_secret_from_directory(self, secret_name: str, path: str, namespace: str, secret_type: str = 'generic',
                                     only_files: List[str] = None):
        files = [fname for fname in os.listdir(path) if os.path.isfile(os.path.join(path, fname)) and
                 (not only_files or fname in only_files)]
        cmd = f'create secret {secret_type} {secret_name} ' + \
              ' '.join([f'--from-file={fname}={os.path.join(path, fname)}' for fname in files])
        self.kubectl(cmd, namespace=namespace)

    @property
    def monitoring_prometheus_pod(self):
        for pod in KubernetesOps.list_pods(self, namespace='monitoring'):
            for container in pod.spec.containers:
                if container.name == 'prometheus':
                    return pod
        return None


class BasePodContainer(cluster.BaseNode):
    parent_cluster: PodCluster
    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 5  # minutes
    pod_terminate_timeout = 5  # minutes

    def __init__(self, name: str, parent_cluster: PodCluster, node_prefix: str = "node", node_index: int = 1,
                 base_logdir: Optional[str] = None, dc_idx: int = 0, rack=0):
        self.node_index = node_index
        cluster.BaseNode.__init__(
            self, name=name,
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            dc_idx=dc_idx,
            rack=rack)

    @cached_property
    def pod_replace_timeout(self) -> int:
        return self.pod_terminate_timeout + self.pod_readiness_timeout

    @staticmethod
    def is_docker() -> bool:
        return True

    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):
        self.remoter = KubernetesCmdRunner(kluster=self.parent_cluster.k8s_cluster,
                                           pod=self.name,
                                           container=self.parent_cluster.container,
                                           namespace=self.parent_cluster.namespace)

    def _init_port_mapping(self):
        pass

    @property
    def system_log(self):
        return os.path.join(self.logdir, "system.log")

    @property
    def region(self):
        return self.parent_cluster.k8s_cluster.datacenter[0]  # TODO: find the node and return it's region.

    def start_journal_thread(self):
        self._journal_thread = get_system_logging_thread(logs_transport="kubectl",
                                                         node=self,
                                                         target_log_file=self.system_log)
        if self._journal_thread:
            self.log.info("Use %s as logging daemon", type(self._journal_thread).__name__)
            self._journal_thread.start()
        else:
            TestFrameworkEvent(source=self.__class__.__name__,
                               source_method="start_journal_thread",
                               message="Got no logging daemon by unknown reason").publish()

    def check_spot_termination(self):
        pass

    @property
    def _pod(self):
        pods = KubernetesOps.list_pods(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                       field_selector=f"metadata.name={self.name}")
        return pods[0] if pods else None

    @property
    def _pod_status(self):
        if pod := self._pod:
            return pod.status
        return None

    @property
    def _cluster_ip_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _loadbalancer_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}-loadbalancer")
        return services[0] if services else None

    @property
    def _svc(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _container_status(self):
        pod_status = self._pod_status
        if pod_status:
            return next((x for x in pod_status.container_statuses if x.name == self.parent_cluster.container), None)
        return None

    def _refresh_instance_state(self):
        public_ips = []
        private_ips = []

        if cluster_ip_service := self._cluster_ip_service:
            cluster_ip = cluster_ip_service.spec.cluster_ip
            private_ips.append(cluster_ip)

        if pod_status := self._pod_status:
            public_ips.append(pod_status.host_ip)
            private_ips.append(pod_status.pod_ip)
        return (public_ips or [None, ], private_ips or [None, ])

    @property
    def image(self) -> str:
        return self._container_status.image

    def _get_ipv6_ip_address(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError("Not implemented yet")  # TODO: implement this method.

    def hard_reboot(self):
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete pod {self.name} --now',
            namespace='scylla',
            timeout=self.pod_terminate_timeout * 60 + 10)

    def soft_reboot(self):
        # Kubernetes brings pods back to live right after it is deleted
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete pod {self.name} --grace-period={self.pod_terminate_timeout * 60}',
            namespace='scylla',
            timeout=self.pod_terminate_timeout * 60 + 10)

    # On kubernetes there is no stop/start, closest analog of node restart would be soft_restart
    restart = soft_reboot

    @property
    def uptime(self):
        # update -s from inside of docker containers shows docker host uptime
        return datetime.fromtimestamp(int(self.remoter.run('stat -c %Y /proc/1', ignore_status=True).stdout.strip()))

    def start_coredump_thread(self):
        self._coredump_thread = CoredumpExportFileThread(
            self, self._maximum_number_of_cores_to_publish, ['/var/lib/scylla/coredumps'])
        self._coredump_thread.start()

    @cached_property
    def node_name(self) -> str:
        return self._pod.spec.node_name

    @property
    def instance_name(self) -> str:
        return self.node_name

    def terminate_k8s_node(self):
        """
        Delete kubernetes node, which will terminate scylla node that is running on it
        """
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete node {self.node_name} --now',
            timeout=self.pod_terminate_timeout * 60 + 10)

    def terminate_k8s_host(self):
        """
        Terminate kubernetes node via cloud API (like GCE/EC2), which will terminate scylla node that is running on it
        """
        raise NotImplementedError("To be overridden in child class")

    def is_kubernetes(self) -> bool:
        return True


class BaseScyllaPodContainer(BasePodContainer):
    parent_cluster: ScyllaPodCluster

    @contextlib.contextmanager
    def remote_scylla_yaml(self, path: str = cluster.SCYLLA_YAML_PATH) -> ContextManager:
        """Update scylla.yaml, k8s way

        Scylla Operator handles scylla.yaml updates using ConfigMap resource and we don't need to update it
        manually on each node.  Just collect all required changes to parent_cluster.scylla_yaml dict and if it
        differs from previous one, set parent_cluster.scylla_yaml_update_required flag.  No actual changes done here.
        Need to do cluster rollout restart.

        More details here: https://github.com/scylladb/scylla-operator/blob/master/docs/generic.md#configure-scylla
        """
        with self.parent_cluster.scylla_yaml_lock:
            scylla_yaml_copy = deepcopy(self.parent_cluster.scylla_yaml)
            yield self.parent_cluster.scylla_yaml
            if scylla_yaml_copy == self.parent_cluster.scylla_yaml:
                LOGGER.debug("%s: scylla.yaml hasn't been changed", self)
                return
            original = yaml.safe_dump(scylla_yaml_copy).splitlines(keepends=True)
            changed = yaml.safe_dump(self.parent_cluster.scylla_yaml).splitlines(keepends=True)
            diff = "".join(unified_diff(original, changed))
            LOGGER.debug("%s: scylla.yaml requires to be updated with:\n%s", self, diff)
            self.parent_cluster.scylla_yaml_update_required = True

    @cluster.log_run_info
    def start_scylla_server(self, verify_up=True, verify_down=False,
                            timeout=500, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run('supervisorctl start scylla', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

    @cluster.log_run_info
    @retrying(n=3, sleep_time=5, allowed_exceptions=NETWORK_EXCEPTIONS + (CommandTimedOut, ),
              message="Failed to stop scylla.server, retrying...")
    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300,
                           ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('supervisorctl stop scylla',
                         timeout=timeout, ignore_status=ignore_status)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    @property
    def node_name(self) -> str:
        return self._pod.spec.node_name

    def restart_scylla_server(self, verify_up_before=False, verify_up_after=True, timeout=300, ignore_status=False):
        if verify_up_before:
            self.wait_db_up(timeout=timeout)
        self.remoter.run("supervisorctl restart scylla", timeout=timeout)
        if verify_up_after:
            self.wait_db_up(timeout=timeout)

    @cluster.log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=300):
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)

    @property
    def scylla_listen_address(self):
        pod_status = self._pod_status
        return pod_status and pod_status.pod_ip

    def init(self) -> None:
        super().init()
        if self.distro.is_rhel_like:
            self.remoter.sudo("rpm -q iproute || yum install -y iproute")  # need this because of scylladb/scylla#7560
        self.remoter.sudo('mkdir -p /var/lib/scylla/coredumps', ignore_status=True)

    def drain_k8s_node(self):
        """
        Gracefully terminating kubernetes host and return it back to life.
        It terminates scylla node that is running on it
        """
        self.log.info('drain_k8s_node: kubernetes node will be drained, the following is affected :\n' + dedent('''
            GCE instance  -
            K8s node      X  <-
            Scylla Pod    X
            Scylla node   X
            '''))
        k8s_node_name = self.node_name
        self.parent_cluster.k8s_cluster.kubectl(
            f'drain {k8s_node_name} -n scylla --ignore-daemonsets --delete-local-data')
        time.sleep(5)
        self.parent_cluster.k8s_cluster.kubectl(f'uncordon {k8s_node_name}')

    def _restart_node_with_resharding(self, murmur3_partitioner_ignore_msb_bits: int = 12):
        # Change murmur3_partitioner_ignore_msb_bits parameter to cause resharding.
        self.stop_scylla()
        with self.remote_scylla_yaml() as scylla_yml:
            scylla_yml["murmur3_partitioner_ignore_msb_bits"] = murmur3_partitioner_ignore_msb_bits
        self.parent_cluster.update_scylla_config()
        self.soft_reboot()
        search_reshard = self.follow_system_log(patterns=['Reshard', 'Reshap'])
        self.wait_db_up(timeout=self.pod_readiness_timeout * 60)
        return search_reshard

    @property
    def is_seed(self) -> bool:
        try:
            return 'scylla/seed' in self._svc.metadata.labels
        except Exception:
            return False

    @is_seed.setter
    def is_seed(self, value):
        pass

    @property
    def k8s_pod_uid(self) -> str:
        try:
            return str(self._pod.metadata.uid)
        except Exception:
            return ''

    def wait_till_k8s_pod_get_uid(self, timeout: int = None, ignore_uid=None) -> str:
        """
        Wait till pod get any valid uid.
        If ignore_uid is provided it wait till any valid uid different from ignore_uid
        """
        if timeout is None:
            timeout = self.pod_replace_timeout
        wait_for(lambda: self.k8s_pod_uid and self.k8s_pod_uid != ignore_uid, timeout=timeout,
                 text=f"Wait till host {self} get uid")
        return self.k8s_pod_uid

    def mark_to_be_replaced(self, overwrite: bool = False):
        if self.is_seed:
            raise RuntimeError("Scylla-operator does not support seed nodes replacement")
        # Mark pod with label that is going to be picked up by scylla-operator, pod to be removed and reconstructed
        cmd = f'label svc {self.name} scylla/replace=""'
        if overwrite:
            cmd += ' --overwrite'
        self.parent_cluster.k8s_cluster.kubectl(cmd, namespace=self.parent_cluster.namespace, ignore_status=True)

    def wait_for_svc(self):
        wait_for(self._wait_for_svc,
                 text=f"Wait for k8s service {self.name} to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_svc(self):
        self.parent_cluster.k8s_cluster.kubectl(
            f"get svc {self.name}", namespace=self.parent_cluster.namespace, verbose=False)
        return True

    def wait_for_k8s_node_readiness(self):
        wait_for(self._wait_for_k8s_node_readiness,
                 text=f"Wait for k8s host {self.node_name} to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_k8s_node_readiness(self):
        if self.node_name is None:
            raise RuntimeError(f"Can't find node for pod {self.name}")
        result = self.parent_cluster.k8s_cluster.kubectl(
            f"wait node --timeout={self.pod_readiness_timeout // 3}m --for=condition=Ready {self.node_name}",
            namespace=self.parent_cluster.namespace,
            timeout=self.pod_readiness_timeout // 3 * 60 + 10
        )
        if result.stdout.count('condition met') != 1:
            raise RuntimeError('Node is not reported as ready')
        return True

    def wait_for_pod_to_appear(self):
        wait_for(self._wait_for_pod_to_appear,
                 text="Wait for pod(s) to apear...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_pod_to_appear(self):
        if self._pod is None:
            raise RuntimeError('Pod is not reported as ready')
        return True

    def wait_for_pod_readiness(self):
        time.sleep(self.pod_readiness_delay)

        # To make it more informative in worst case scenario it repeat waiting text 5 times
        wait_for(self._wait_for_pod_readiness,
                 text="Wait for pod(s) to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_pod_readiness(self):
        result = self.parent_cluster.k8s_cluster.kubectl(
            f"wait --timeout={self.pod_readiness_timeout // 3}m --for=condition=Ready pod {self.name}",
            namespace=self.parent_cluster.namespace,
            timeout=self.pod_readiness_timeout // 3 * 60 + 10)
        if result.stdout.count('condition met') != 1:
            raise RuntimeError('Pod is not reported as ready')
        return True

    def refresh_ip_address(self):
        # Invalidate ip address cache
        old_ip_info = (self.public_ip_address, self.private_ip_address)
        self._private_ip_address_cached = self._public_ip_address_cached = self._ipv6_ip_address_cached = None

        if old_ip_info == (self.public_ip_address, self.private_ip_address):
            return

        self._init_port_mapping()


class PodCluster(cluster.BaseCluster):
    PodContainerClass: Type[BasePodContainer] = BasePodContainer

    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 namespace: str = "default",
                 container: Optional[str] = None,
                 cluster_uuid: Optional[str] = None,
                 cluster_prefix: str = "cluster",
                 node_prefix: str = "node",
                 node_type: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:
        self.k8s_cluster = k8s_cluster
        self.namespace = namespace
        self.container = container

        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=k8s_cluster.datacenter,
                         node_type=node_type)

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Namespace: {self.namespace}"

    @property
    def k8s_apps_v1_api(self):
        return self.k8s_cluster.k8s_apps_v1_api

    @property
    def k8s_core_v1_api(self):
        return self.k8s_cluster.k8s_core_v1_api

    def _create_node(self, node_index: int, pod_name: str, dc_idx: int, rack: int) -> BasePodContainer:
        node = self.PodContainerClass(parent_cluster=self,
                                      name=pod_name,
                                      base_logdir=self.logdir,
                                      node_prefix=self.node_prefix,
                                      node_index=node_index,
                                      dc_idx=dc_idx,
                                      rack=rack)
        node.init()
        return node

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        # PodCluster only register new nodes and return whatever was registered
        k8s_pods = KubernetesOps.list_pods(self, namespace=self.namespace)
        nodes = []
        for pod in k8s_pods:
            if not any((x for x in pod.status.container_statuses if x.name == self.container)):
                continue
            is_already_registered = False
            for node in self.nodes:
                if node.name == pod.metadata.name:
                    is_already_registered = True
                    break
            if is_already_registered:
                continue
            # TBD: A rack validation might be needed
            # Register a new node
            node = self._create_node(len(self.nodes), pod.metadata.name, dc_idx, rack)
            nodes.append(node)
            self.nodes.append(node)
        if len(nodes) != count:
            raise RuntimeError(
                f'Requested {count} number of nodes to add, while only {len(nodes)} new nodes where found')
        return nodes

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Derived class must implement 'get_node_ips_param' method!")

    def wait_for_init(self):
        raise NotImplementedError("Derived class must implement 'wait_for_init' method!")

    def get_nodes_reboot_timeout(self, count) -> Union[float, int]:
        """
        Return readiness timeout (in minutes) for case when nodes are restarted
        sums out readiness and terminate timeouts for given nodes
        """
        return count * self.PodContainerClass.pod_readiness_timeout

    @cached_property
    def get_nodes_readiness_delay(self) -> Union[float, int]:
        return self.PodContainerClass.pod_readiness_delay

    def wait_for_pods_readiness(self, pods_to_wait: int, total_pods: int):
        time.sleep(self.get_nodes_readiness_delay)
        readiness_timeout = self.get_nodes_reboot_timeout(pods_to_wait)

        @timeout(message="Wait for pod(s) to be ready...", timeout=readiness_timeout * 60)
        def wait_cluster_is_ready():
            # To make it more informative in worst case scenario made it repeat 5 times, by readiness_timeout // 5
            result = self.k8s_cluster.kubectl(
                f"wait --timeout={readiness_timeout // 5}m --all --for=condition=Ready pod",
                namespace=self.namespace,
                timeout=readiness_timeout // 5 * 60 + 10)
            if result.stdout.count('condition met') != total_pods:
                raise RuntimeError('Not all nodes reported')

        wait_cluster_is_ready()


class ScyllaPodCluster(cluster.BaseScyllaCluster, PodCluster):
    node_setup_requires_scylla_restart = False

    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 scylla_cluster_config: str,
                 scylla_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:
        k8s_cluster.deploy_scylla_cluster(
            scylla_cluster_config,
            target_mgmt_agent_to_minio=bool(params.get('use_mgmt'))
        )
        self.scylla_yaml_lock = RLock()
        self.scylla_yaml = {}
        self.scylla_yaml_update_required = False
        self.scylla_cluster_name = scylla_cluster_name
        super().__init__(k8s_cluster=k8s_cluster,
                         namespace="scylla",
                         container="scylla",
                         cluster_prefix=cluster.prepend_user_prefix(user_prefix, 'db-cluster'),
                         node_prefix=cluster.prepend_user_prefix(user_prefix, 'db-node'),
                         node_type="scylla-db",
                         n_nodes=n_nodes,
                         params=params)

    get_scylla_args = cluster_docker.ScyllaDockerCluster.get_scylla_args

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, *_, **__):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)
        if self.scylla_yaml_update_required:
            self.update_scylla_config()
            time.sleep(30)
            self.add_sidecar_injection()
            self.restart_scylla()
            self.scylla_yaml_update_required = False
            self.wait_for_nodes_up_and_normal(nodes=node_list)

    @property
    def _k8s_scylla_cluster_api(self) -> Resource:
        return KubernetesOps.dynamic_api(self.k8s_cluster.dynamic_client,
                                         api_version=SCYLLA_API_VERSION,
                                         kind=SCYLLA_CLUSTER_RESOURCE_KIND)

    def replace_scylla_cluster_value(self, path: str, value: Any) -> Optional[ANY_KUBERNETES_RESOURCE]:
        LOGGER.debug("Replace `%s' with `%s' in %s's spec", path, value, self.scylla_cluster_name)
        return self._k8s_scylla_cluster_api.patch(body=[{"op": "replace", "path": path, "value": value}],
                                                  name=self.scylla_cluster_name,
                                                  namespace=self.namespace,
                                                  content_type=JSON_PATCH_TYPE)

    def get_scylla_cluster_value(self, path: str) -> Optional[ANY_KUBERNETES_RESOURCE]:
        """
        Get scylla cluster value from kubernetes API.
        """
        cluster_data = self._k8s_scylla_cluster_api.get(namespace=self.namespace, name=self.scylla_cluster_name)
        return walk_thru_data(cluster_data, path)

    def get_scylla_cluster_plain_value(self, path: str) -> Union[Dict, List, str, None]:
        """
        Get scylla cluster value from kubernetes API and converts result to basic python data types.
        Use it if you are going to modify the data.
        """
        cluster_data = self._k8s_scylla_cluster_api.get(
            namespace=self.namespace, name=self.scylla_cluster_name).to_dict()
        return walk_thru_data(cluster_data, path)

    def add_scylla_cluster_value(self, path: str, element: Any):
        init = self.get_scylla_cluster_value(path) is None
        if path.endswith('/'):
            path = path[0:-1]
        if init:
            # You can't add to empty array, so you need to replace it
            op = "replace"
            path = path
            value = [element]
        else:
            op = "add"
            path = path + "/-"
            value = element
        self._k8s_scylla_cluster_api.patch(
            body=[
                {
                    "op": op,
                    "path": path,
                    "value": value
                }
            ],
            name=self.scylla_cluster_name,
            namespace=self.namespace,
            content_type=JSON_PATCH_TYPE
        )

    @property
    def scylla_cluster_spec(self) -> ResourceField:
        return self.get_scylla_cluster_value('/spec')

    def update_seed_provider(self):
        pass

    def install_scylla_manager(self, node):
        pass

    @cached_property
    def scylla_manager_cluster_name(self):
        return f"{self.namespace}/{self.scylla_cluster_name}"

    @property
    def scylla_manager_node(self):
        return self.k8s_cluster.scylla_manager_cluster.nodes[0]

    def get_cluster_manager(self, create_cluster_if_not_exists: bool = False) -> AnyManagerCluster:
        return super().get_cluster_manager(create_cluster_if_not_exists=create_cluster_if_not_exists)

    def create_cluster_manager(self, cluster_name: str, manager_tool=None, host_ip=None):
        self.log.info('Scylla manager should not be manipulated on kubernetes manually')
        self.log.info('Instead of creating new cluster we will wait for 5 minutes till it get registered automatically')
        raise NotImplementedError('Scylla manager should not be manipulated on kubernetes manually')

    def node_config_setup(self,
                          node,
                          seed_address=None,
                          endpoint_snitch=None,
                          murmur3_partitioner_ignore_msb_bits=None,
                          client_encrypt=None):  # pylint: disable=too-many-arguments,invalid-name
        if client_encrypt is None:
            client_encrypt = self.params.get("client_encrypt")

        if client_encrypt:
            raise NotImplementedError("client_encrypt is not supported by k8s-* backends yet")

        if self.get_scylla_args():
            raise NotImplementedError("custom SCYLLA_ARGS is not supported by k8s-* backends yet")

        if self.params.get("server_encrypt"):
            raise NotImplementedError("server_encrypt is not supported by k8s-* backends yet")

        append_scylla_yaml = self.params.get("append_scylla_yaml")

        if append_scylla_yaml:
            unsupported_options = ("system_key_directory", "system_info_encryption", "kmip_hosts:", )
            if any(substr in append_scylla_yaml for substr in unsupported_options):
                raise NotImplementedError(
                    f"{unsupported_options} are not supported in append_scylla_yaml by k8s-* backends yet")

        node.config_setup(enable_exp=self.params.get("experimental"),
                          endpoint_snitch=endpoint_snitch,
                          authenticator=self.params.get("authenticator"),
                          server_encrypt=self.params.get("server_encrypt"),
                          append_scylla_yaml=append_scylla_yaml,
                          hinted_handoff=self.params.get("hinted_handoff"),
                          authorizer=self.params.get("authorizer"),
                          alternator_port=self.params.get("alternator_port"),
                          murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits,
                          alternator_enforce_authorization=self.params.get("alternator_enforce_authorization"),
                          internode_compression=self.params.get("internode_compression"),
                          ldap=self.params.get('use_ldap_authorization'))

    def validate_seeds_on_all_nodes(self):
        pass

    def set_seeds(self, wait_for_timeout=300, first_only=False):
        assert self.nodes, "DB cluster should have at least 1 node"
        self.nodes[0].is_seed = True

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        self._create_k8s_rack_if_not_exists(rack)
        current_members = self.scylla_cluster_spec.datacenter.racks[rack].members
        self.replace_scylla_cluster_value(f"/spec/datacenter/racks/{rack}/members", current_members + count)

        # Wait while whole cluster (on all racks) including new nodes are up and running
        self.wait_for_pods_readiness(pods_to_wait=count, total_pods=len(self.nodes) + count)
        return super().add_nodes(count=count,
                                 ec2_user_data=ec2_user_data,
                                 dc_idx=dc_idx,
                                 rack=rack,
                                 enable_auto_bootstrap=enable_auto_bootstrap)

    def _create_k8s_rack_if_not_exists(self, rack: int):
        if self.get_scylla_cluster_value(f'/spec/datacenter/racks/{rack}') is not None:
            return
        # Create new rack of very first rack of the cluster
        new_rack = self.get_scylla_cluster_plain_value('/spec/datacenter/racks/0')
        new_rack['members'] = 0
        new_rack['name'] = f'{new_rack["name"]}-{rack}'
        self.add_scylla_cluster_value('/spec/datacenter/racks', new_rack)

    def _delete_k8s_rack(self, rack: int):
        racks = self.get_scylla_cluster_plain_value(f'/spec/datacenter/racks/')
        if len(racks) == 1:
            return
        racks.pop(rack)
        self.replace_scylla_cluster_value('/spec/datacenter/racks', racks)

    def terminate_node(self, node: BasePodContainer):
        raise NotImplementedError("Kubernetes can't terminate nodes")

    def decommission(self, node):
        rack = node.rack
        assert self.get_rack_nodes(rack)[-1] == node, "Can withdraw the last node only"
        current_members = self.scylla_cluster_spec.datacenter.racks[rack].members

        self.replace_scylla_cluster_value(f"/spec/datacenter/racks/{rack}/members", current_members - 1)
        self.k8s_cluster.kubectl(f"wait --timeout={node.pod_terminate_timeout}m --for=delete pod {node.name}",
                                 namespace=self.namespace,
                                 timeout=node.pod_terminate_timeout * 60 + 10)
        super().terminate_node(node)
        # TBD: Enable rack deletion after https://github.com/scylladb/scylla-operator/issues/287 is resolved
        # if current_members - 1 == 0:
        #     self.delete_rack(rack)

        if monitors := cluster.Setup.tester_obj().monitors:
            monitors.reconfigure_scylla_monitoring()

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.replace_scylla_cluster_value("/spec/version", new_version)
        new_image = f"{self.params.get('docker_image')}:{new_version}"

        if not self.nodes:
            return True

        @timeout(timeout=self.nodes[0].pod_replace_timeout * 2 * 60)
        def wait_till_any_node_get_new_image(nodes_with_old_image: list):
            for node in nodes_with_old_image.copy():
                if node.image == new_image:
                    nodes_with_old_image.remove(node)
                    return True
            time.sleep(self.PodContainerClass.pod_readiness_delay)
            raise RuntimeError('No node was upgraded')

        nodes = self.nodes.copy()
        while nodes:
            wait_till_any_node_get_new_image(nodes)

        self.wait_for_pods_readiness(len(self.nodes), len(self.nodes))

    def update_scylla_config(self):
        with self.scylla_yaml_lock:
            with NamedTemporaryFile("w", delete=False) as tmp:
                tmp.write(yaml.safe_dump(self.scylla_yaml))
                tmp.flush()
                self.k8s_cluster.kubectl_multi_cmd(
                    f'kubectl create configmap scylla-config --from-file=scylla.yaml={tmp.name} ||'
                    f'kubectl create configmap scylla-config --from-file=scylla.yaml={tmp.name} -o yaml '
                    '--dry-run=client | kubectl replace -f -',
                    namespace=self.namespace
                )
            os.remove(tmp.name)

    def add_sidecar_injection(self) -> bool:
        result = False
        for statefulset in self.k8s_apps_v1_api.list_namespaced_stateful_set(namespace=self.namespace).items:
            is_owned_by_scylla_cluster = False
            for owner_reference in statefulset.metadata.owner_references:
                if owner_reference.kind == 'ScyllaCluster' and owner_reference.name == self.scylla_cluster_name:
                    is_owned_by_scylla_cluster = True
                    break

            if not is_owned_by_scylla_cluster:
                self.log.debug(f"add_sidecar_injection: statefulset {statefulset.metadata.name} skipped")
                continue

            if any([container for container in statefulset.spec.template.spec.containers if
                    container.name == 'injected-busybox-sidecar']):
                self.log.debug(
                    f"add_sidecar_injection: statefulset {statefulset.metadata.name} sidecar is already injected")
                continue

            result = True
            statefulset.spec.template.spec.containers.insert(
                0,
                V1Container(
                    command=['/bin/sh', '-c', 'while true; do sleep 1 ; done'],
                    image='busybox:1.32.0',
                    name='injected-busybox-sidecar'
                )
            )

            self.k8s_apps_v1_api.patch_namespaced_stateful_set(
                statefulset.metadata.name, self.namespace,
                {
                    'spec': {
                        'template': {
                            'spec': {
                                'containers': statefulset.spec.template.spec.containers
                            }
                        }
                    }
                }
            )
            self.log.info(f"add_sidecar_injection: statefulset {statefulset.metadata.name} sidecar has been injected")
        return result

    def check_cluster_health(self):
        if self.params.get('k8s_deploy_monitoring'):
            self._check_kubernetes_monitoring_health()
        super().check_cluster_health()

    def _check_kubernetes_monitoring_health(self, max_diff=0.1):
        from sdcm.cluster import Setup

        self.log.debug('Check kubernetes monitoring health')

        kubernetes_prometheus_host = None
        try:
            kubernetes_prometheus_host = self.k8s_cluster.monitoring_prometheus_pod.status.pod_ip
            kubernetes_prometheus = PrometheusDBStats(host=kubernetes_prometheus_host)
        except Exception as exc:
            ClusterHealthValidatorEvent.MonitoringStatus(
                message=f'Failed to connect to kubernetes prometheus server at {kubernetes_prometheus_host},'
                        f' due to the: {exc}').publish()

            ClusterHealthValidatorEvent.Done(message="Kubernetes monitoring health check finished").publish()
            return

        monitoring_prometheus_host = None
        try:
            monitoring_prometheus_host = Setup.tester_obj().monitors.nodes[0].external_address
            monitoring_prometheus = PrometheusDBStats(host=monitoring_prometheus_host)
        except Exception as exc:
            ClusterHealthValidatorEvent.MonitoringStatus(
                message=f'Failed to connect to monitoring prometheus server at {monitoring_prometheus_host},'
                        f' due to the: {exc}').publish()

            ClusterHealthValidatorEvent.Done(message="Kubernetes monitoring health check finished").publish()
            return

        end_time = time.time()
        start_time = end_time - 60 * 30

        @timeout(timeout=10, message='Getting stats from prometheus')
        def average(stat_method, params):
            data = stat_method(*params)
            if not data:
                return 0
            return sum([int(val) for _, val in data if val.isdigit()]) / len(data)

        def get_stat(name, source, method_name, params):
            try:
                return average(getattr(source, method_name), params)
            except Exception as exc:
                ClusterHealthValidatorEvent.MonitoringStatus(
                    message=f'Failed to get data from {name}: {exc}').publish()
                ClusterHealthValidatorEvent.Done(message="Kubernetes monitoring health check finished").publish()
                return

        errors = []
        for method_name, metric_params in {
            'get_latency': [start_time, end_time, "read"],
            'get_throughput': [start_time, end_time],
        }.items():
            val1 = kubernetes_data = get_stat(
                'kubernetes monitoring', kubernetes_prometheus, method_name, metric_params)
            val2 = monitoring_data = get_stat('sct monitoring', monitoring_prometheus, method_name, metric_params)

            if val2 is None or val1 is None:
                return

            if val2 == 0:
                if val1 == 0:
                    continue
                else:
                    val2, val1 = val1, val2

            if (val2 - val1) / val2 > val2 * max_diff:
                errors.append(
                    f'Metric {method_name} on sct monitoring {monitoring_data}, '
                    f'which is more than 1% different from kubernetes monitoring result {kubernetes_data}')

        for error in errors:
            ClusterHealthValidatorEvent.MonitoringStatus(message=error).publish()

        ClusterHealthValidatorEvent.Done(message="Kubernetes monitoring health check finished").publish()

    def restart_scylla(self, nodes=None, random=False):
        # TODO: add support for the "nodes" param to have compatible logic with
        # other backends.
        self.k8s_cluster.kubectl("rollout restart statefulset", namespace=self.namespace)
        readiness_timeout = self.get_nodes_reboot_timeout(len(self.nodes))
        statefulsets = KubernetesOps.list_statefulsets(self.k8s_cluster, namespace=self.namespace)
        if random:
            statefulsets = librandom.sample(statefulsets, len(statefulsets))
        for statefulset in statefulsets:
            self.k8s_cluster.kubectl(
                f"rollout status statefulset/{statefulset.metadata.name} "
                f"--watch=true --timeout={readiness_timeout}m",
                namespace=self.namespace,
                timeout=readiness_timeout * 60 + 10)


class LoaderPodCluster(cluster.BaseLoaderSet, PodCluster):
    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 loader_cluster_config: str,
                 loader_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:

        self.loader_cluster_config = loader_cluster_config
        self.loader_cluster_name = loader_cluster_name
        self.loader_cluster_created = False

        cluster.BaseLoaderSet.__init__(self, params=params)
        PodCluster.__init__(self,
                            k8s_cluster=k8s_cluster,
                            namespace="sct-loaders",
                            container="cassandra-stress",
                            cluster_prefix=cluster.prepend_user_prefix(user_prefix, "loader-set"),
                            node_prefix=cluster.prepend_user_prefix(user_prefix, "loader-node"),
                            node_type="loader",
                            n_nodes=n_nodes,
                            params=params)

    def node_setup(self,
                   node: BasePodContainer,
                   verbose: bool = False,
                   db_node_address: Optional[str] = None,
                   **kwargs) -> None:

        self.install_scylla_bench(node)

        if self.params.get('client_encrypt'):
            node.config_client_encrypt()

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:

        if self.loader_cluster_created:
            raise NotImplementedError("Changing number of nodes in LoaderPodCluster is not supported.")

        self.k8s_cluster.deploy_loaders_cluster(self.loader_cluster_config)
        self.wait_for_pods_readiness(pods_to_wait=count, total_pods=count)
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)
        self.loader_cluster_created = True

        return new_nodes
