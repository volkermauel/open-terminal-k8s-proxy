"""Tests for pod/PVC/service manifest builders."""

from typing import Any

from terminal_proxy.config import PodMode, Settings, StorageMode
from terminal_proxy.k8s.pod_builder import build_pod_for_user, build_service_manifest
from terminal_proxy.models import TerminalPod


def _cfg(**kw: Any) -> Settings:
    base: dict[str, Any] = {
        "proxy_api_key": "k",
        "namespace": "ns",
        "terminal_image": "img:1",
        "terminal_image_pull_policy": "IfNotPresent",
    }
    base.update(kw)
    return Settings(**base)


def _container(pod_manifest: dict[str, Any]) -> Any:
    return pod_manifest["spec"]["containers"][0]


def test_cwd_and_home_set_to_data_mount_with_pvc() -> None:
    cfg = _cfg(storage_mode=StorageMode.PER_USER)
    pod, _pvc, _sec, _svc = build_pod_for_user(TerminalPod.create("u", "k"), cfg)
    c = _container(pod)
    assert c["args"] == ["--cwd", "/data"]
    assert next(e for e in c["env"] if e["name"] == "HOME")["value"] == "/data"


def test_cwd_and_home_set_to_home_user_without_pvc() -> None:
    cfg = _cfg(storage_mode=StorageMode.NONE)
    pod, _pvc, _sec, _svc = build_pod_for_user(TerminalPod.create("u", "k"), cfg)
    c = _container(pod)
    assert c["args"] == ["--cwd", "/home/user"]
    assert next(e for e in c["env"] if e["name"] == "HOME")["value"] == "/home/user"


def test_data_mount_path_config_used() -> None:
    cfg = _cfg(storage_mode=StorageMode.PER_USER, data_mount_path="/workspace")
    pod, *_ = build_pod_for_user(TerminalPod.create("u", "k"), cfg)
    assert _container(pod)["args"] == ["--cwd", "/workspace"]


def test_perchat_pod_has_initcontainer_and_chat_labels() -> None:
    cfg = _cfg(storage_mode=StorageMode.SHARED, pod_mode=PodMode.PER_CHAT)
    t = TerminalPod.create("u", "k", "chat-42")
    pod, _pvc, _sec, _svc = build_pod_for_user(t, cfg)
    c = _container(pod)

    # cwd targets the chat subdir on the shared volume
    assert c["args"] == ["--cwd", "/data/chat-42"]
    # chat label + annotation
    assert pod["metadata"]["labels"].get("chat-id-hash") == t.chat_hash
    assert pod["metadata"]["annotations"]["chat-id"] == "chat-42"
    # initContainer creates the chat dir before the main container starts
    inits = pod["spec"].get("initContainers", [])
    assert len(inits) == 1
    assert inits[0]["name"] == "init-chat-dir"
    assert "mkdir -p /data/chat-42" in inits[0]["command"][2]
    assert inits[0]["volumeMounts"]


def test_peruser_pod_has_no_initcontainer() -> None:
    cfg = _cfg(storage_mode=StorageMode.PER_USER)
    pod, *_ = build_pod_for_user(TerminalPod.create("u", "k"), cfg)
    assert "initContainers" not in pod["spec"]


def test_chat_pod_names_under_63_chars() -> None:
    cfg = _cfg(storage_mode=StorageMode.SHARED, pod_mode=PodMode.PER_CHAT)
    t = TerminalPod.create("a-very-long-user-id-xyz", "k", "a-very-long-chat-id-abc")
    assert len(t.pod_name) <= 63
    assert len(t.secret_name) <= 63
    pod, *_ = build_pod_for_user(t, cfg)
    assert len(pod["metadata"]["name"]) <= 63


def test_service_selector_includes_chat_hash_for_chat_pod() -> None:
    cfg = _cfg(storage_mode=StorageMode.SHARED, pod_mode=PodMode.PER_CHAT)
    t = TerminalPod.create("u", "k", "chat-1")
    _pod, _pvc, _sec, svc = build_pod_for_user(t, cfg)
    assert svc["spec"]["selector"].get("chat-id-hash") == t.chat_hash
    assert svc["metadata"]["labels"].get("chat-id-hash") == t.chat_hash


def test_service_selector_user_pod_unaffected() -> None:
    cfg = _cfg(storage_mode=StorageMode.PER_USER)
    t = TerminalPod.create("u", "k")
    svc = build_service_manifest(t, cfg)
    assert "chat-id-hash" not in svc["spec"]["selector"]
