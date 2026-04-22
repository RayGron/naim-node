#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import sys
from pathlib import Path


def strip_comment(line):
    quote = None
    escaped = False
    out = []
    for char in line:
        if escaped:
            out.append(char)
            escaped = False
            continue
        if char == "\\":
            out.append(char)
            escaped = True
            continue
        if quote:
            out.append(char)
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            out.append(char)
            continue
        if char == "#":
            break
        out.append(char)
    return "".join(out).rstrip()


def parse_scalar(value):
    value = value.strip()
    if value == "":
        return ""
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    lowered = value.lower()
    if lowered in ("true", "yes", "on"):
        return True
    if lowered in ("false", "no", "off"):
        return False
    if lowered in ("null", "none", "~"):
        return None
    try:
        return int(value)
    except ValueError:
        return value


def parse_simple_yaml(text):
    tokens = []
    for raw in text.splitlines():
        line = strip_comment(raw)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        tokens.append((indent, line.strip()))

    def parse_block(index, indent):
        if index >= len(tokens):
            return {}, index
        if tokens[index][0] < indent:
            return {}, index
        if tokens[index][1].startswith("- "):
            return parse_list(index, indent)
        return parse_map(index, indent)

    def parse_map(index, indent):
        result = {}
        while index < len(tokens):
            current_indent, content = tokens[index]
            if current_indent < indent:
                break
            if current_indent > indent:
                raise ValueError(f"unexpected indentation near: {content}")
            if content.startswith("- "):
                break
            key, sep, rest = content.partition(":")
            if not sep:
                raise ValueError(f"expected key/value near: {content}")
            key = key.strip()
            rest = rest.strip()
            index += 1
            if rest:
                result[key] = parse_scalar(rest)
            elif index < len(tokens) and tokens[index][0] > current_indent:
                result[key], index = parse_block(index, tokens[index][0])
            else:
                result[key] = {}
        return result, index

    def parse_list(index, indent):
        result = []
        while index < len(tokens):
            current_indent, content = tokens[index]
            if current_indent < indent:
                break
            if current_indent != indent or not content.startswith("- "):
                break
            item_text = content[2:].strip()
            index += 1
            if not item_text:
                if index < len(tokens) and tokens[index][0] > current_indent:
                    item, index = parse_block(index, tokens[index][0])
                else:
                    item = None
            elif ":" in item_text:
                item = {}
                key, _, rest = item_text.partition(":")
                key = key.strip()
                rest = rest.strip()
                if rest:
                    item[key] = parse_scalar(rest)
                elif index < len(tokens) and tokens[index][0] > current_indent:
                    item[key], index = parse_block(index, tokens[index][0])
                else:
                    item[key] = {}
                if index < len(tokens) and tokens[index][0] > current_indent:
                    extra, index = parse_map(index, tokens[index][0])
                    item.update(extra)
            else:
                item = parse_scalar(item_text)
            result.append(item)
        return result, index

    parsed, index = parse_block(0, tokens[0][0] if tokens else 0)
    if index != len(tokens):
        raise ValueError(f"could not parse inventory near: {tokens[index][1]}")
    return parsed


def load_inventory(path):
    text = Path(path).read_text(encoding="utf-8")
    if path.endswith(".json"):
        return json.loads(text)
    try:
        import yaml  # type: ignore

        loaded = yaml.safe_load(text)
        return loaded or {}
    except ModuleNotFoundError:
        return parse_simple_yaml(text)


def normalize_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def first_present(mapping, *keys, default=""):
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


def selected_worker(inventory, worker_name):
    workers = inventory.get("workers") or []
    if not workers:
        return {}
    if worker_name:
        for worker in workers:
            if str(worker.get("name", "")) == worker_name:
                return worker
        raise SystemExit(f"worker '{worker_name}' was not found in inventory")
    return workers[0]


def shell_export(name, value):
    if isinstance(value, bool):
        value = "yes" if value else "no"
    elif value is None:
        value = ""
    else:
        value = str(value)
    return f"export {name}={shlex.quote(value)}"


def env_override(name, value):
    current = os.environ.get(name)
    return current if current not in (None, "") else value


def command_env(args):
    inventory = load_inventory(args.inventory)
    registry = inventory.get("registry") or {}
    control = inventory.get("control") or {}
    worker = selected_worker(inventory, args.worker)

    hostd_public_url = first_present(control, "hostd_public_url", "hostd_url")
    if not hostd_public_url:
        hostd_public_host = first_present(control, "hostd_public_host", "host", default="")
        hostd_public_port = first_present(control, "hostd_public_port", default=18080)
        if hostd_public_host:
            hostd_public_url = f"http://{hostd_public_host}:{hostd_public_port}"

    controller_url = first_present(worker, "controller_url", default=hostd_public_url)

    exports = {
        "NAIM_REGISTRY": env_override(
            "NAIM_REGISTRY", first_present(registry, "url", "registry")
        ),
        "NAIM_REGISTRY_PROJECT": env_override(
            "NAIM_REGISTRY_PROJECT", first_present(registry, "project")
        ),
        "NAIM_IMAGE_TAG": env_override(
            "NAIM_IMAGE_TAG", first_present(registry, "tag", "image_tag")
        ),
        "NAIM_REGISTRY_USERNAME": env_override(
            "NAIM_REGISTRY_USERNAME", first_present(registry, "username", "user")
        ),
        "NAIM_REGISTRY_PASSWORD_FILE": env_override(
            "NAIM_REGISTRY_PASSWORD_FILE", first_present(registry, "password_file")
        ),
        "NAIM_REGISTRY_PASSWORD_COMMAND": env_override(
            "NAIM_REGISTRY_PASSWORD_COMMAND", first_present(registry, "password_command")
        ),
        "NAIM_REGISTRY_PASSWORD_FILE_ON_CONTROL": first_present(
            registry, "password_file_on_control"
        ),
        "NAIM_HARBOR_CONFIG_ON_CONTROL": first_present(
            registry, "harbor_config_on_control"
        ),
        "NAIM_CONTROL_SSH": first_present(control, "ssh"),
        "NAIM_CONTROL_ROOT": first_present(control, "root"),
        "NAIM_CONTROL_DOMAIN": first_present(control, "domain"),
        "NAIM_CONTROL_CONTROLLER_LOCAL_PORT": first_present(
            control, "controller_local_port"
        ),
        "NAIM_CONTROL_WEB_UI_LOCAL_PORT": first_present(control, "web_ui_local_port"),
        "NAIM_CONTROL_HOSTD_PUBLIC_PORT": first_present(control, "hostd_public_port"),
        "NAIM_MAIN_SSH": first_present(control, "ssh"),
        "NAIM_MAIN_ROOT": first_present(control, "root"),
        "NAIM_MAIN_CONTROLLER_LOCAL_PORT": first_present(
            control, "controller_local_port"
        ),
        "NAIM_MAIN_WEB_UI_LOCAL_PORT": first_present(control, "web_ui_local_port"),
        "NAIM_MAIN_HOSTD_PUBLIC_PORT": first_present(control, "hostd_public_port"),
    }

    if worker:
        nvidia = first_present(worker, "nvidia", "enable_nvidia", default=True)
        worker_exports = {
            "NAIM_WORKER_NAME": first_present(worker, "name"),
            "NAIM_WORKER_SSH": first_present(worker, "ssh"),
            "NAIM_WORKER_SSH_PORT": first_present(worker, "ssh_port", default=22),
            "NAIM_WORKER_ROOT": first_present(worker, "root"),
            "NAIM_WORKER_SHARED_ROOT": first_present(worker, "shared_root"),
            "NAIM_WORKER_CONTROLLER_URL": controller_url,
            "NAIM_WORKER_POLL_INTERVAL_SEC": first_present(
                worker, "poll_interval_sec", default=2
            ),
            "NAIM_WORKER_INVENTORY_SCAN_INTERVAL_SEC": first_present(
                worker, "inventory_scan_interval_sec", default=30
            ),
            "NAIM_WORKER_ENABLE_NVIDIA": "yes" if normalize_bool(nvidia) else "no",
            "NAIM_HPC1_SSH": first_present(worker, "ssh"),
            "NAIM_HPC1_SSH_PORT": first_present(worker, "ssh_port", default=22),
            "NAIM_HOSTD_NODE": first_present(worker, "name"),
            "NAIM_HOSTD_ROOT": first_present(worker, "root"),
            "NAIM_HOSTD_SHARED_ROOT": first_present(worker, "shared_root"),
            "NAIM_HOSTD_CONTROLLER_URL": controller_url,
            "NAIM_HOSTD_POLL_INTERVAL_SEC": first_present(
                worker, "poll_interval_sec", default=2
            ),
            "NAIM_HOSTD_INVENTORY_SCAN_INTERVAL_SEC": first_present(
                worker, "inventory_scan_interval_sec", default=30
            ),
            "NAIM_HOSTD_ENABLE_NVIDIA": "yes" if normalize_bool(nvidia) else "no",
        }
        exports.update(worker_exports)

    for key, value in exports.items():
        if value != "":
            print(shell_export(key, value))


def command_workers(args):
    inventory = load_inventory(args.inventory)
    for worker in inventory.get("workers") or []:
        name = worker.get("name")
        if name:
            print(name)


def command_json(args):
    print(json.dumps(load_inventory(args.inventory), indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    env_parser = subparsers.add_parser("env")
    env_parser.add_argument("--inventory", required=True)
    env_parser.add_argument("--worker", default="")
    env_parser.set_defaults(func=command_env)

    workers_parser = subparsers.add_parser("workers")
    workers_parser.add_argument("--inventory", required=True)
    workers_parser.set_defaults(func=command_workers)

    json_parser = subparsers.add_parser("json")
    json_parser.add_argument("--inventory", required=True)
    json_parser.set_defaults(func=command_json)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
