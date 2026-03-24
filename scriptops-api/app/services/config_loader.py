"""
Load servers and script registry from YAML (optional) merged with built-in defaults.
Paths: SCRIPTOPS_CONFIG_DIR or package-relative config/, SCRIPTOPS_SERVERS_FILE, SCRIPTOPS_SCRIPTS_FILE.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

from app.models.schemas import ScriptCategory
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


class ServerConfig(BaseModel):
    id: str
    host: str
    ssh_port: int = Field(22, ge=1, le=65535)
    ssh_user: str = "deploy"
    ssh_key_path: str = "/etc/scriptops/keys/deploy.pem"


class ScriptEntry(BaseModel):
    id: str
    name: str
    category: ScriptCategory
    server: str
    path: str
    interpreter: str
    min_role: str
    timeout_sec: int = 300
    allowed_params: List[str] = Field(default_factory=list)

    @field_validator("category", mode="before")
    @classmethod
    def coerce_category(cls, v: Any) -> Any:
        if isinstance(v, str):
            return ScriptCategory(v)
        return v


def _default_config_dir() -> Path:
    env = os.environ.get("SCRIPTOPS_CONFIG_DIR")
    if env:
        return Path(env).resolve()
    # scriptops-api/config next to app package
    here = Path(__file__).resolve().parent.parent.parent
    return here / "config"


def _servers_path() -> Path:
    if os.environ.get("SCRIPTOPS_SERVERS_FILE"):
        return Path(os.environ["SCRIPTOPS_SERVERS_FILE"]).resolve()
    return _default_config_dir() / "servers.yaml"


def _scripts_path() -> Path:
    if os.environ.get("SCRIPTOPS_SCRIPTS_FILE"):
        return Path(os.environ["SCRIPTOPS_SCRIPTS_FILE"]).resolve()
    return _default_config_dir() / "scripts.yaml"


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        logger.warning("Config file not found: %s — using built-ins only", path)
        return None
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_SERVERS: Dict[str, ServerConfig] = {}
_SCRIPT_REGISTRY: Dict[str, dict] = {}


def load_servers() -> Dict[str, ServerConfig]:
    global _SERVERS
    data = _read_yaml(_servers_path())
    servers: Dict[str, ServerConfig] = {}
    if data and "servers" in data:
        for row in data["servers"]:
            s = ServerConfig.model_validate(row)
            servers[s.id] = s
    _SERVERS = servers
    logger.info("Loaded %d server(s) from config", len(_SERVERS))
    return _SERVERS


def get_server(server_id: str) -> Optional[ServerConfig]:
    if not _SERVERS:
        load_servers()
    return _SERVERS.get(server_id)


def _builtin_registry() -> Dict[str, dict]:
    """Import from executor without circular import by lazy import."""
    from app.services import executor as ex

    return dict(ex.BUILTIN_SCRIPT_REGISTRY)


def load_script_registry() -> Dict[str, dict]:
    """
    Merge built-in registry with scripts.yaml (YAML wins on id collision).
    Values are dicts compatible with SCRIPT_REGISTRY usage (category as ScriptCategory enum).
    """
    global _SCRIPT_REGISTRY
    builtin = _builtin_registry()
    data = _read_yaml(_scripts_path())
    merged: Dict[str, dict] = {k: dict(v) for k, v in builtin.items()}
    if data and "scripts" in data:
        for row in data["scripts"]:
            ent = ScriptEntry.model_validate(row)
            merged[ent.id] = {
                "name": ent.name,
                "category": ent.category,
                "server": ent.server,
                "path": ent.path,
                "interpreter": ent.interpreter,
                "min_role": ent.min_role,
                "timeout_sec": ent.timeout_sec,
                "allowed_params": list(ent.allowed_params),
            }
    logger.info("Script registry: %d script(s)", len(merged))
    return merged
