"""memory-tencentdb Memory Provider — MemoryProvider interface for Hermes.

Four-layer memory system (L0 conversation, L1 extraction, L2 scene blocks,
L3 persona synthesis) accessed via local Node.js Gateway sidecar.

The Gateway runs the memory-tencentdb Core engine (the same engine used by
the OpenClaw plugin) as an HTTP service. This provider translates Hermes
lifecycle events into Gateway API calls.

v3 migration: data-plane calls now use /v3/* endpoints with
team_id / agent_id / user_id tenancy isolation.

Config via environment variables:
  MEMORY_TENCENTDB_GATEWAY_HOST — Gateway host (default: 127.0.0.1)
  MEMORY_TENCENTDB_GATEWAY_PORT — Gateway port (default: 8420)
  MEMORY_TENCENTDB_GATEWAY_CMD  — Command to start the Gateway (optional; if
                                  unset, the provider auto-discovers
                                  ``src/gateway/server.ts`` next to the plugin
                                  checkout or under ``$HOME``)
  MEMORY_TENCENTDB_KNOWLEDGE_HOST — MemoryKnowledge (wiki) host (default: 127.0.0.1)
  MEMORY_TENCENTDB_KNOWLEDGE_PORT — MemoryKnowledge (wiki) port (default: 8421)
  MEMORY_TENCENTDB_KNOWLEDGE_TEAM — Team whose wiki libraries are listed
                                     (default: default; reads are not
                                     team-scoped, so every profile sees the
                                     same corpus)

The on-disk data directory (L0~L3 storage) is owned by the Gateway, not by
this provider. Point the Gateway at a custom location with ``TDAI_DATA_DIR``
(read directly by ``src/gateway/config.ts``); otherwise it falls back to
``~/.memory-tencentdb/memory-tdai`` (with legacy fallback to ``~/memory-tdai``
if it still exists). This provider no longer carries its own data-dir default
or env var — a single source of truth prevents the two layers from drifting
apart.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent.memory_provider import MemoryProvider

from .client import MemoryKnowledgeWikiClient, MemoryTencentdbSdkClient
from .supervisor import GatewaySupervisor

logger = logging.getLogger(__name__)

# Circuit breaker: after N consecutive failures, pause API calls
_BREAKER_THRESHOLD = 5
_BREAKER_COOLDOWN_SECS = 60

# Gateway resurrect throttle: minimum seconds between two consecutive
# ensure_running() attempts triggered by in-flight request failures.
# Chosen smaller than _BREAKER_COOLDOWN_SECS so we can try to revive the
# Gateway *within* a breaker-open window (otherwise the breaker would mask
# the outage for a full minute before we'd even attempt recovery).
# Chosen larger than supervisor's HEALTH_CHECK_MAX_WAIT (30s) so a failed
# revive never overlaps with the next attempt.
_RECOVER_COOLDOWN_SECS = 15

# Background sync thread limits.
# _MAX_INFLIGHT_SYNCS caps concurrent capture threads: once reached we wait
# on the oldest one with _SYNC_JOIN_TIMEOUT_SECS before spawning a new one,
# so a hung Gateway can't cause unbounded thread growth.
_MAX_INFLIGHT_SYNCS = 4
_SYNC_JOIN_TIMEOUT_SECS = 5.0
# _SHUTDOWN_JOIN_TIMEOUT_SECS bounds how long shutdown will wait on *each*
# still-alive sync thread. Kept per-thread rather than global because one
# stuck thread shouldn't starve the rest.
_SHUTDOWN_JOIN_TIMEOUT_SECS = 5.0

# Watchdog: a daemon thread that periodically inspects the Gateway and
# resurrects it on death.
_WATCHDOG_INTERVAL_SECS = 10.0
_WATCHDOG_SHUTDOWN_TIMEOUT_SECS = 2.0

# Gateway networking defaults (kept here so is_available/initialize stay in sync)
_DEFAULT_GATEWAY_HOST = "127.0.0.1"
_DEFAULT_GATEWAY_PORT = 8420

# Default tenancy IDs for v3 isolation.
_DEFAULT_TEAM_ID = "team-ohgfaxy8um"
_DEFAULT_AGENT_ID = "default"
_DEFAULT_USER_ID = "default"

# 共享事实层 agent_id：存放跨 profile 通用的事实（渲染参数、端口映射、公司背景、
# 模型选型等）。各 profile 检索时先查自己的 agent 层，命中不足再回退到这里。
_SHARED_FACT_AGENT_ID = "default"

# MemoryKnowledge (wiki service) networking defaults. The wiki corpus lives in
# a separate process from the memory Gateway, so it gets its own host/port.
_DEFAULT_KNOWLEDGE_HOST = "127.0.0.1"
_DEFAULT_KNOWLEDGE_PORT = 8421
# Team used when listing wiki libraries. The corpus itself is NOT team-scoped
# for reads (``/v3/tools/call`` keys off ``knowledge_id`` alone), so every
# Hermes profile sees the same libraries regardless of this value.
_DEFAULT_KNOWLEDGE_TEAM = "default"
# How many wiki libraries a cross-library search fans out to.
_MAX_WIKI_FANOUT = 8


def _resolve_knowledge_port(default: int = _DEFAULT_KNOWLEDGE_PORT) -> int:
    """Resolve MEMORY_TENCENTDB_KNOWLEDGE_PORT with validation."""
    raw = os.environ.get("MEMORY_TENCENTDB_KNOWLEDGE_PORT")
    if raw is None or not raw.strip():
        return default
    try:
        port = int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid MEMORY_TENCENTDB_KNOWLEDGE_PORT=%r (not an integer); "
            "falling back to default %d.",
            raw, default,
        )
        return default
    if not (1 <= port <= 65535):
        logger.warning(
            "MEMORY_TENCENTDB_KNOWLEDGE_PORT=%d is out of range (1..65535); "
            "falling back to default %d.",
            port, default,
        )
        return default
    return port


def _resolve_knowledge_host(default: str = _DEFAULT_KNOWLEDGE_HOST) -> str:
    """Resolve MEMORY_TENCENTDB_KNOWLEDGE_HOST, trimming whitespace."""
    raw = os.environ.get("MEMORY_TENCENTDB_KNOWLEDGE_HOST")
    if raw is None:
        return default
    host = raw.strip()
    return host or default


def _resolve_knowledge_team(default: str = _DEFAULT_KNOWLEDGE_TEAM) -> str:
    """Resolve MEMORY_TENCENTDB_KNOWLEDGE_TEAM (which team's libraries to list)."""
    raw = os.environ.get("MEMORY_TENCENTDB_KNOWLEDGE_TEAM")
    if raw is None:
        return default
    team = raw.strip()
    return team or default


def _resolve_user_id(default: str = _DEFAULT_USER_ID) -> str:
    """
    Resolve the user_id dimension for memory writes.

    Priority:
      1. Explicit ``user_id`` kwarg from Hermes core (chat sessions carry it).
      2. ``MEMORY_TENCENTDB_USER_ID`` env — global override, so scheduled
         jobs (which have no user context) write under the same user_id
         as interactive sessions. Without this, cron output lands under
         "default" and becomes invisible to every later read.
      3. ``HERMES_SESSION_USER_ID`` env — per-session value.
      4. Fallback to ``default``.
    """
    for var in ("MEMORY_TENCENTDB_USER_ID", "HERMES_SESSION_USER_ID"):
        raw = os.environ.get(var)
        if raw and raw.strip():
            return raw.strip()
    return default


def _resolve_gateway_port(default: int = _DEFAULT_GATEWAY_PORT) -> int:
    """Resolve MEMORY_TENCENTDB_GATEWAY_PORT with validation."""
    raw = os.environ.get("MEMORY_TENCENTDB_GATEWAY_PORT")
    if raw is None or not raw.strip():
        return default
    try:
        port = int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid MEMORY_TENCENTDB_GATEWAY_PORT=%r (not an integer); "
            "falling back to default %d.",
            raw, default,
        )
        return default
    if not (1 <= port <= 65535):
        logger.warning(
            "MEMORY_TENCENTDB_GATEWAY_PORT=%d is out of range (1..65535); "
            "falling back to default %d.",
            port, default,
        )
        return default
    return port


def _resolve_gateway_host(default: str = _DEFAULT_GATEWAY_HOST) -> str:
    """Resolve MEMORY_TENCENTDB_GATEWAY_HOST, trimming whitespace."""
    raw = os.environ.get("MEMORY_TENCENTDB_GATEWAY_HOST")
    if raw is None:
        return default
    host = raw.strip()
    return host or default


def _resolve_gateway_api_key() -> Optional[str]:
    """Read the optional Gateway Bearer token from the environment."""
    for var in ("MEMORY_TENCENTDB_GATEWAY_API_KEY", "TDAI_GATEWAY_API_KEY"):
        raw = os.environ.get(var)
        if raw is None:
            continue
        value = raw.strip()
        if value:
            return value
    return None


# Candidate locations searched by _discover_gateway_cmd() when the user has not
# set MEMORY_TENCENTDB_GATEWAY_CMD. Order matters: in-tree checkout (next to
# this file) wins over ad-hoc clones in ``$HOME``.
_GATEWAY_DISCOVERY_RELATIVE_PATHS = (
    Path("src") / "gateway" / "server.ts",
)
_GATEWAY_DISCOVERY_HOME_PATHS = (
    Path(".memory-tencentdb") / "tdai-memory-openclaw-plugin" / "src" / "gateway" / "server.ts",
    Path("tdai-memory-openclaw-plugin") / "src" / "gateway" / "server.ts",
    Path(".hermes") / "plugins" / "tdai-memory-openclaw-plugin" / "src" / "gateway" / "server.ts",
)


def _discover_gateway_cmd() -> Optional[str]:
    """Best-effort fallback to locate the Node Gateway entry point."""
    import shlex

    here = Path(__file__).resolve()
    plugin_root_candidates: List[Path] = []
    try:
        plugin_root_candidates.append(here.parents[3])
    except IndexError:
        pass

    home_raw = os.environ.get("HOME") or os.environ.get("USERPROFILE")
    home = Path(home_raw) if home_raw else None

    searched: List[Path] = []
    for root in plugin_root_candidates:
        for rel in _GATEWAY_DISCOVERY_RELATIVE_PATHS:
            searched.append(root / rel)
    if home is not None:
        for rel in _GATEWAY_DISCOVERY_HOME_PATHS:
            searched.append(home / rel)

    for candidate in searched:
        try:
            if candidate.is_file():
                plugin_root = candidate.parents[2]
                logger.info(
                    "memory-tencentdb Gateway command auto-discovered: %s "
                    "(override with MEMORY_TENCENTDB_GATEWAY_CMD)",
                    candidate,
                )
                inner = (
                    f"cd {shlex.quote(str(plugin_root))} && "
                    "exec pnpm exec tsx src/gateway/server.ts"
                )
                return f"sh -c {shlex.quote(inner)}"
        except OSError:
            continue

    logger.debug(
        "memory-tencentdb Gateway auto-discovery found no server.ts under: %s",
        ", ".join(str(p) for p in searched) or "<no candidates>",
    )
    return None


# Search tool limit bounds (shared by memory_search and conversation_search).
_DEFAULT_SEARCH_LIMIT = 5
_MAX_SEARCH_LIMIT = 20


def _coerce_limit(
    raw: Any,
    *,
    default: int = _DEFAULT_SEARCH_LIMIT,
    maximum: int = _MAX_SEARCH_LIMIT,
) -> int:
    """Coerce a tool-call ``limit`` arg into a valid int in ``[1, maximum]``."""
    if raw is None or raw == "":
        return default
    if isinstance(raw, bool):
        logger.warning(
            "memory-tencentdb: ignoring non-numeric limit=%r (bool); "
            "falling back to default %d.",
            raw, default,
        )
        return default
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        logger.warning(
            "memory-tencentdb: ignoring invalid limit=%r (not numeric); "
            "falling back to default %d.",
            raw, default,
        )
        return default
    if value < 1:
        return 1
    if value > maximum:
        return maximum
    return value


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

MEMORY_SEARCH_SCHEMA = {
    "name": "memory_tencentdb_memory_search",
    "description": (
        "Search through the user's long-term memories. Use this when you need to "
        "recall specific information about the user's preferences, past events, "
        "instructions, or context from previous conversations. Returns relevant "
        "memory records ranked by relevance."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query describing what you want to recall about the user.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of results to return (default: 5, max: 20).",
            },
            "type": {
                "type": "string",
                "enum": ["persona", "episodic", "instruction"],
                "description": "Optional filter by memory type.",
            },
        },
        "required": ["query"],
    },
}

CONVERSATION_SEARCH_SCHEMA = {
    "name": "memory_tencentdb_conversation_search",
    "description": (
        "Search through past conversation history (raw dialogue records). "
        "Use when memory_tencentdb_memory_search doesn't have the information "
        "you need, or when you want to find specific past conversations or "
        "exact words the user said before."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query describing what conversation content you want to find.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of messages to return (default: 5, max: 20).",
            },
        },
        "required": ["query"],
    },
}

READ_SCENE_SCHEMA = {
    "name": "memory_tencentdb_read_scene",
    "description": (
        "Read a scene block's full content by its name. "
        "Use this when you see a scene listed in the available scenes and want to "
        "retrieve detailed information from that scene."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "scene_id": {
                "type": "string",
                "description": "Scene file name (e.g. 'travel-plan.md' or 'travel-plan').",
            },
        },
        "required": ["scene_id"],
    },
}

WIKI_SEARCH_SCHEMA = {
    "name": "memory_tencentdb_wiki_search",
    "description": (
        "Search structured knowledge-base (wiki) pages — domain reference material "
        "such as engineering runbooks, spec sheets, and troubleshooting procedures. "
        "Use this for 'how do I do X' / 'what is the spec for Y' questions about a "
        "documented domain; use memory_tencentdb_memory_search instead for facts "
        "about the user or past conversations. Optionally narrow to one library; "
        "omit knowledge_id to search every visible library."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query (keywords work best).",
            },
            "knowledge_id": {
                "type": "string",
                "description": (
                    "Optional library id (e.g. 'wiki-u9olfgjd'). Omit to search all "
                    "visible libraries. Use memory_tencentdb_wiki_list to see them."
                ),
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of results per library (default: 5, max: 20).",
            },
        },
        "required": ["query"],
    },
}

WIKI_LIST_SCHEMA = {
    "name": "memory_tencentdb_wiki_list",
    "description": (
        "List the available knowledge-base libraries (wiki ids, names, page counts) "
        "so you can target a specific one with memory_tencentdb_wiki_search."
    ),
    "parameters": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

WIKI_READ_SCHEMA = {
    "name": "memory_tencentdb_wiki_read",
    "description": (
        "Read the full content of a knowledge-base page returned by "
        "memory_tencentdb_wiki_search (use its 'path' field)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "knowledge_id": {
                "type": "string",
                "description": "Library id the page belongs to (e.g. 'wiki-u9olfgjd').",
            },
            "path": {
                "type": "string",
                "description": "Page path as returned by search, e.g. 'wiki/concepts/光猫桥接模式.md'.",
            },
        },
        "required": ["knowledge_id", "path"],
    },
}


# ---------------------------------------------------------------------------
# MemoryProvider implementation
# ---------------------------------------------------------------------------

class MemoryTencentdbProvider(MemoryProvider):
    """memory-tencentdb four-layer memory via local Gateway sidecar."""

    def __init__(self):
        self._supervisor: Optional[GatewaySupervisor] = None
        self._client: Optional[MemoryTencentdbSdkClient] = None
        self._wiki_client: Optional[MemoryKnowledgeWikiClient] = None
        self._session_id = ""
        self._user_id = _DEFAULT_USER_ID
        self._team_id = _DEFAULT_TEAM_ID
        self._agent_id = _DEFAULT_AGENT_ID
        self._gateway_available = False
        self._initialized = False

        # Background sync threads.
        self._sync_lock = threading.Lock()
        self._active_syncs: List[threading.Thread] = []

        # Circuit breaker
        self._consecutive_failures = 0
        self._breaker_open_until = 0.0

        # Gateway auto-resurrect state.
        self._recover_lock = threading.Lock()
        self._last_recover_attempt = float("-inf")

        # Watchdog state.
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()

    # -- Properties -----------------------------------------------------------

    @property
    def name(self) -> str:
        return "memory_tencentdb"

    # -- Circuit breaker ------------------------------------------------------

    def _is_breaker_open(self) -> bool:
        if self._consecutive_failures < _BREAKER_THRESHOLD:
            return False
        if time.monotonic() >= self._breaker_open_until:
            self._consecutive_failures = 0
            return False
        return True

    def _record_success(self):
        self._consecutive_failures = 0

    def _record_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= _BREAKER_THRESHOLD:
            self._breaker_open_until = time.monotonic() + _BREAKER_COOLDOWN_SECS
            logger.warning(
                "memory-tencentdb circuit breaker tripped after %d failures. Pausing for %ds.",
                self._consecutive_failures, _BREAKER_COOLDOWN_SECS,
            )

    # -- Gateway auto-resurrect ----------------------------------------------

    def _try_recover_gateway(self, *, bypass_cooldown: bool = False) -> bool:
        """Best-effort: re-probe and, if needed, re-launch the Gateway."""
        supervisor = self._supervisor
        if supervisor is None:
            return False

        if not bypass_cooldown:
            now = time.monotonic()
            if now - self._last_recover_attempt < _RECOVER_COOLDOWN_SECS:
                return False

        if not self._recover_lock.acquire(blocking=False):
            return False

        try:
            supervisor = self._supervisor
            if supervisor is None:
                return False

            if not bypass_cooldown:
                now = time.monotonic()
                if now - self._last_recover_attempt < _RECOVER_COOLDOWN_SECS:
                    return False

            if supervisor.is_running():
                logger.info(
                    "memory-tencentdb Gateway is reachable again; restoring provider state."
                )
                ok = True
            else:
                logger.warning(
                    "memory-tencentdb Gateway appears down; attempting to resurrect."
                )
                ok = supervisor.ensure_running()

            self._last_recover_attempt = time.monotonic()

            if ok:
                self._client = supervisor.client
                self._gateway_available = True
                self._consecutive_failures = 0
                self._breaker_open_until = 0.0
                logger.info("memory-tencentdb Gateway recovery succeeded.")
                return True

            logger.warning(
                "memory-tencentdb Gateway recovery failed; will retry no sooner than %ds.",
                _RECOVER_COOLDOWN_SECS,
            )
            return False
        except Exception as e:
            self._last_recover_attempt = time.monotonic()
            logger.warning("memory-tencentdb Gateway recovery raised: %s", e)
            return False
        finally:
            self._recover_lock.release()

    # -- Watchdog & lazy probe -----------------------------------------------

    def _ensure_alive_for_request(self) -> bool:
        """Lazy probe used by the request short-circuit guards."""
        if self._gateway_available:
            return True
        if self._is_breaker_open():
            return False
        self._try_recover_gateway()
        return self._gateway_available

    def _start_watchdog(self) -> None:
        """Start the background watchdog thread (idempotent)."""
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="memory-tencentdb-watchdog",
        )
        self._watchdog_thread = thread
        thread.start()

    def _watchdog_loop(self) -> None:
        """Periodically verify Gateway health and resurrect on death."""
        logger.debug(
            "memory-tencentdb watchdog started (interval=%.1fs)",
            _WATCHDOG_INTERVAL_SECS,
        )
        while not self._watchdog_stop.wait(timeout=_WATCHDOG_INTERVAL_SECS):
            try:
                supervisor = self._supervisor
                if supervisor is None:
                    break

                if self._gateway_available and supervisor.is_process_alive():
                    continue

                healthy = False
                try:
                    healthy = supervisor.is_running()
                except Exception as e:
                    logger.debug(
                        "memory-tencentdb watchdog health probe raised: %s", e,
                    )

                if healthy:
                    if not self._gateway_available:
                        logger.info(
                            "memory-tencentdb watchdog: Gateway is reachable; "
                            "restoring provider state."
                        )
                        self._client = supervisor.client
                        self._gateway_available = True
                        self._consecutive_failures = 0
                        self._breaker_open_until = 0.0
                    continue

                logger.warning(
                    "memory-tencentdb watchdog: Gateway unreachable; "
                    "attempting to resurrect."
                )
                self._try_recover_gateway(bypass_cooldown=True)
            except Exception as e:
                logger.warning(
                    "memory-tencentdb watchdog iteration raised (continuing): %s", e,
                )

        logger.debug("memory-tencentdb watchdog exiting")

    def _stop_watchdog(self) -> None:
        """Signal the watchdog to exit and join briefly. Safe if not started."""
        self._watchdog_stop.set()
        thread = self._watchdog_thread
        self._watchdog_thread = None
        if thread is None:
            return
        thread.join(timeout=_WATCHDOG_SHUTDOWN_TIMEOUT_SECS)
        if thread.is_alive():
            logger.debug(
                "memory-tencentdb watchdog did not exit within %.1fs; "
                "abandoning (daemon).",
                _WATCHDOG_SHUTDOWN_TIMEOUT_SECS,
            )

    # -- Core lifecycle -------------------------------------------------------

    def is_available(self) -> bool:
        """Check if the Gateway is configured or already running."""
        if os.environ.get("MEMORY_TENCENTDB_GATEWAY_CMD"):
            return True
        if os.environ.get("MEMORY_TENCENTDB_GATEWAY_PORT"):
            return True
        host = _resolve_gateway_host()
        port = _resolve_gateway_port()
        api_key = _resolve_gateway_api_key()
        client = MemoryTencentdbSdkClient(
            base_url=f"http://{host}:{port}",
            timeout=2,
            api_key=api_key,
            service_id="default",
        )
        try:
            result = client.health(timeout=2)
            return result.get("status") in ("ok", "degraded")
        except Exception:
            return False

    def initialize(self, session_id: str, **kwargs) -> None:
        """Start or connect to the Gateway sidecar.

        v3: accepts team_id, agent_id, user_id for tenancy isolation.
        All default to "default".
        """
        self._session_id = session_id
        # 显式 kwarg 优先；缺失时走环境变量回落（cronjob 等无用户上下文的场景）
        self._user_id = kwargs.get("user_id") or _resolve_user_id()
        self._team_id = kwargs.get("team_id", _DEFAULT_TEAM_ID)
        # Map Hermes core's agent_identity (profile name) to agent_id for proper
        # per-profile tenancy isolation. Falls back to agent_id kwarg for explicit
        # config, then to DEFAULT_AGENT_ID ("default").
        self._agent_id = kwargs.get("agent_identity") or kwargs.get("agent_id", _DEFAULT_AGENT_ID)

        # Wiki service (MemoryKnowledge) — a separate process from the memory
        # Gateway, so it is never supervised by this provider. The corpus is
        # shared across profiles, so every profile builds the same client.
        self._wiki_client = MemoryKnowledgeWikiClient(
            base_url=f"http://{_resolve_knowledge_host()}:{_resolve_knowledge_port()}",
            timeout=10,
        )

        host = _resolve_gateway_host()
        port = _resolve_gateway_port()
        gateway_cmd = os.environ.get("MEMORY_TENCENTDB_GATEWAY_CMD") or _discover_gateway_cmd()
        api_key = _resolve_gateway_api_key()

        self._supervisor = GatewaySupervisor(
            host=host,
            port=port,
            gateway_cmd=gateway_cmd,
            api_key=api_key,
        )

        self._initialized = True

        def _background_start():
            try:
                available = self._supervisor.ensure_running()
                if available:
                    self._client = self._supervisor.client
                    self._gateway_available = True
                    logger.info(
                        "memory-tencentdb Gateway ready (background start, %s:%d)",
                        host, port,
                    )
                else:
                    logger.warning(
                        "memory-tencentdb Gateway not available after background start. "
                        "Memory features will be disabled until the Gateway is reachable."
                    )
            except Exception as e:
                logger.warning(
                    "memory-tencentdb background Gateway start failed (non-fatal): %s", e
                )

        if self._supervisor.is_running():
            self._client = self._supervisor.client
            self._gateway_available = True
            logger.info(
                "memory-tencentdb Gateway already running (%s:%d)",
                host, port,
            )
        else:
            t = threading.Thread(
                target=_background_start, daemon=True,
                name="tdai-gateway-init",
            )
            t.start()

        self._start_watchdog()

    def system_prompt_block(self) -> str:
        wiki_line = (
            "A shared knowledge base (wiki) is also available: use "
            "memory_tencentdb_wiki_search for domain reference material (runbooks, "
            "specs, procedures) and memory_tencentdb_wiki_list to see libraries."
        )
        if not self._gateway_available:
            return wiki_line
        return (
            "# memory-tencentdb Memory\n"
            f"Active. Team: {self._team_id}, Agent: {self._agent_id}, User: {self._user_id}.\n"
            "Four-layer memory system (L0→L1→L2→L3) with automatic conversation "
            "capture, structured memory extraction, scene blocks, and persona synthesis.\n"
            "Use memory_tencentdb_memory_search to find specific memories, "
            "memory_tencentdb_conversation_search to search raw conversation history, "
            "memory_tencentdb_read_scene to read detailed scene content.\n"
            + wiki_line
        )

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Synchronous recall — fetch memories in real-time for the current turn.

        v3: parallel calls to atomic/search (L1) + core/read (L3) + scenario/ls (L2).
        """
        if not query:
            return ""
        if not self._ensure_alive_for_request() or not self._client:
            return ""

        effective_session = session_id or self._session_id
        try:
            # Parallel fetch: L1 memories + L3 core + L2 scene navigation
            results: Dict[str, Any] = {}
            errors: List[str] = []

            def _fetch(label: str, fn):
                try:
                    results[label] = fn()
                except Exception as e:
                    errors.append(f"{label}: {e}")

            threads = [
                threading.Thread(
                    target=_fetch,
                    args=("l1", lambda: self._client.atomic_search(
                        query=query,
                        limit=5,
                        team_id=self._team_id,
                        agent_id=self._agent_id,
                        user_id=self._user_id,
                    )),
                    daemon=True,
                ),
                threading.Thread(
                    target=_fetch,
                    args=("l3", lambda: self._client.core_read(
                        team_id=self._team_id,
                        agent_id=self._agent_id,
                        user_id=self._user_id,
                    )),
                    daemon=True,
                ),
                threading.Thread(
                    target=_fetch,
                    args=("l2", lambda: self._client.scenario_ls(
                        team_id=self._team_id,
                        agent_id=self._agent_id,
                        user_id=self._user_id,
                    )),
                    daemon=True,
                ),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=self._client._timeout if self._client else 10)

            if errors:
                logger.warning("memory-tencentdb prefetch partial failures: %s", "; ".join(errors))

            # Build recall context from results
            parts: List[str] = []

            # L1 memories
            l1_data = results.get("l1", {})
            l1_items = l1_data.get("data", {}).get("items", [])
            if l1_items:
                lines = []
                for m in l1_items:
                    mtype = m.get("type", "unknown")
                    content = m.get("content", "")
                    lines.append(f"- [{mtype}] {content}")
                parts.append(
                    "<relevant-memories>\n"
                    "以下是当前对话召回的相关记忆，仅作为参考：\n\n"
                    + "\n".join(lines)
                    + "\n</relevant-memories>"
                )

            # L3 core (persona)
            l3_data = results.get("l3", {})
            core_text = l3_data.get("data", {}).get("content", "")
            if core_text:
                parts.append(f"<user-core>\n{core_text}\n</user-core>")

            # L2 scene navigation
            l2_data = results.get("l2", {})
            l2_entries = l2_data.get("data", {}).get("entries", [])
            if l2_entries:
                lines = []
                for s in l2_entries:
                    name = s.get("path", "").replace(".md", "")
                    lines.append(f"- Scene: {name}")
                parts.append(
                    "<scene-navigation>\n"
                    "Available scenes:\n"
                    + "\n".join(lines)
                    + "\n</scene-navigation>"
                )

            self._record_success()
            return "\n\n".join(parts) if parts else ""
        except Exception as e:
            self._record_failure()
            logger.debug("memory-tencentdb prefetch failed: %s", e)
            self._try_recover_gateway()
            return ""

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """No-op — recall is done synchronously in prefetch()."""
        pass

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        """Send the turn to Gateway for capture (non-blocking).

        v3: uses /v3/conversation/add with messages array.
        """
        if not self._ensure_alive_for_request() or not self._client:
            return

        effective_session = session_id or self._session_id
        client = self._client

        # Build v3 messages array with ISO 8601 timestamps
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        user_ts = now.replace(microsecond=max(0, now.microsecond - 1000)).isoformat().replace("+00:00", "Z")
        assistant_ts = now.isoformat().replace("+00:00", "Z")
        messages = [
            {"role": "user", "content": user_content, "timestamp": user_ts},
            {"role": "assistant", "content": assistant_content, "timestamp": assistant_ts},
        ]

        def _sync():
            try:
                client.conversation_add(
                    messages=messages,
                    session_id=effective_session,
                    team_id=self._team_id,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                )
                self._record_success()
            except Exception as e:
                self._record_failure()
                logger.warning("memory-tencentdb sync failed: %s", e)
                self._try_recover_gateway()

        oldest_to_join: Optional[threading.Thread] = None
        with self._sync_lock:
            self._active_syncs = [t for t in self._active_syncs if t.is_alive()]
            if len(self._active_syncs) >= _MAX_INFLIGHT_SYNCS:
                oldest_to_join = self._active_syncs[0]

        if oldest_to_join is not None:
            oldest_to_join.join(timeout=_SYNC_JOIN_TIMEOUT_SECS)
            if oldest_to_join.is_alive():
                logger.warning(
                    "memory-tencentdb sync backlog: oldest sync thread still "
                    "running after %.1fs; %d in-flight threads tracked. "
                    "Continuing with a new sync; Gateway may be hung.",
                    _SYNC_JOIN_TIMEOUT_SECS, len(self._active_syncs),
                )

        thread = threading.Thread(
            target=_sync, daemon=True, name="memory-tencentdb-sync",
        )
        with self._sync_lock:
            self._active_syncs = [t for t in self._active_syncs if t.is_alive()]
            self._active_syncs.append(thread)
        thread.start()

    def shutdown(self) -> None:
        """Clean shutdown — flush and release resources."""
        self._stop_watchdog()

        with self._sync_lock:
            pending = list(self._active_syncs)
            self._active_syncs.clear()

        for t in pending:
            if not t.is_alive():
                continue
            t.join(timeout=_SHUTDOWN_JOIN_TIMEOUT_SECS)
            if t.is_alive():
                logger.warning(
                    "memory-tencentdb shutdown: sync thread %s still alive "
                    "after %.1fs; abandoning (daemon).",
                    t.name, _SHUTDOWN_JOIN_TIMEOUT_SECS,
                )

        # v3 pipeline auto-handles session end; no explicit call needed.
        # if self._client and self._gateway_available:
        #     try:
        #         self._client.end_session(
        #             session_key=self._session_id,
        #             user_id=self._user_id,
        #         )
        #     except Exception as e:
        #         logger.debug("memory-tencentdb session end failed: %s", e)

        supervisor = self._supervisor
        if supervisor is not None:
            try:
                supervisor.shutdown()
            except Exception as e:
                logger.debug("memory-tencentdb supervisor shutdown failed: %s", e)

        self._client = None
        self._gateway_available = False
        self._initialized = False
        self._supervisor = None

    # -- Tools ----------------------------------------------------------------

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        # The wiki tools talk to MemoryKnowledge, a different service from the
        # memory Gateway, so they are always offered — gating them on the
        # Gateway's health would hide them for no reason.
        return [
            MEMORY_SEARCH_SCHEMA,
            CONVERSATION_SEARCH_SCHEMA,
            READ_SCENE_SCHEMA,
            WIKI_SEARCH_SCHEMA,
            WIKI_LIST_SCHEMA,
            WIKI_READ_SCHEMA,
        ]

    def handle_tool_call(self, tool_name: str, args: Dict[str, Any], **kwargs) -> str:
        # Wiki tools bypass the Gateway health gate: they target MemoryKnowledge
        # (a separate process), so a Gateway outage must not disable them.
        if tool_name in ("memory_tencentdb_wiki_search", "memory_tencentdb_wiki_list", "memory_tencentdb_wiki_read"):
            return self._handle_wiki_tool(tool_name, args)

        self._ensure_alive_for_request()
        if not self._client:
            return json.dumps({
                "error": "memory-tencentdb Gateway is not connected. Memory search is temporarily unavailable.",
                "hint": "The Gateway may still be starting up. Try again in a moment.",
            })
        if self._is_breaker_open():
            return json.dumps({"error": "memory-tencentdb Gateway temporarily unavailable (circuit breaker open)."})

        try:
            if tool_name == "memory_tencentdb_memory_search":
                query = args.get("query", "")
                if not query:
                    return json.dumps({"error": "Missing required parameter: query"})
                limit = _coerce_limit(args.get("limit"))
                result = self._client.atomic_search(
                    query=query,
                    limit=limit,
                    type_filter=args.get("type", ""),
                    team_id=self._team_id,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                )
                self._record_success()
                # Unwrap v3 envelope for LLM consumption
                items = result.get("data", {}).get("items", [])
                # 共享事实层回退：profile 专属记忆不足时，从 default（共享事实层）
                # 补查。default 层存放跨 profile 通用的事实（渲染参数、端口映射、
                # 公司背景等）；profile 层仍只放该 profile 的专属记忆。
                shared_items: List[Dict[str, Any]] = []
                if len(items) < limit and self._agent_id != _SHARED_FACT_AGENT_ID:
                    try:
                        shared = self._client.atomic_search(
                            query=query,
                            limit=limit - len(items),
                            type_filter=args.get("type", ""),
                            team_id=self._team_id,
                            agent_id=_SHARED_FACT_AGENT_ID,
                            user_id=self._user_id,
                        )
                        shared_items = shared.get("data", {}).get("items", [])
                    except Exception as exc:  # 回退失败不影响主路径
                        logger.debug(f"[memory-tencentdb] shared-fact fallback failed: {exc}")
                if not items and not shared_items:
                    return "No memories found for this query."
                lines = []
                for m in items:
                    lines.append(f"- [{m.get('type', '?')}] {m.get('content', '')}")
                for m in shared_items:
                    lines.append(f"- [shared] {m.get('content', '')}")
                return "\n".join(lines)

            if tool_name == "memory_tencentdb_conversation_search":
                query = args.get("query", "")
                if not query:
                    return json.dumps({"error": "Missing required parameter: query"})
                result = self._client.conversation_search(
                    query=query,
                    limit=_coerce_limit(args.get("limit")),
                    team_id=self._team_id,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                )
                self._record_success()
                items = result.get("data", {}).get("items", [])
                if not items:
                    return "No conversations found for this query."
                lines = []
                for m in items:
                    role = m.get("role", "?")
                    content = m.get("content", "")
                    lines.append(f"[{role}] {content}")
                return "\n".join(lines)

            if tool_name == "memory_tencentdb_read_scene":
                scene_id = args.get("scene_id", "")
                if not scene_id:
                    return "Error: scene_id is required"
                path = scene_id if scene_id.endswith(".md") else f"{scene_id}.md"
                result = self._client.scenario_read(
                    path=path,
                    team_id=self._team_id,
                    agent_id=self._agent_id,
                    user_id=self._user_id,
                )
                self._record_success()
                content = result.get("data", {}).get("content", "")
                if not content:
                    return f"Scene '{scene_id}' is empty or not found."
                return content

            return json.dumps({"error": f"Unknown tool: {tool_name}"})

        except Exception as e:
            self._record_failure()
            self._try_recover_gateway()
            return json.dumps({"error": f"Tool call failed: {e}"})

    # -- Wiki tools (MemoryKnowledge service) ---------------------------------

    def _wiki_client_or_error(self):
        """Return (client, error_json). The wiki client is created in
        initialize(); recreate it lazily if the provider was used without one."""
        if self._wiki_client is None:
            self._wiki_client = MemoryKnowledgeWikiClient(
                base_url=f"http://{_resolve_knowledge_host()}:{_resolve_knowledge_port()}",
                timeout=10,
            )
        return self._wiki_client, None

    def _wiki_libraries(self, client) -> List[Dict[str, Any]]:
        """Libraries visible to the configured team, ready for searching."""
        items = client.list_wikis(team_id=_resolve_knowledge_team())
        ready = [w for w in items if w.get("status") == "ready"]
        # Fall back to all libraries when none is marked ready (a fresh install
        # may not have set the flag yet) rather than returning nothing.
        return ready or items

    def _handle_wiki_tool(self, tool_name: str, args: Dict[str, Any]) -> str:
        """Handle the wiki tools. Errors here never touch the memory Gateway's
        circuit breaker — a wiki outage must not disable memory search."""
        client, err = self._wiki_client_or_error()
        if err:
            return err

        try:
            if tool_name == "memory_tencentdb_wiki_list":
                libs = self._wiki_libraries(client)
                if not libs:
                    return "No knowledge libraries are available."
                lines = []
                for w in libs:
                    lines.append(
                        f"- {w.get('wiki_id')} | {w.get('name')} | "
                        f"pages={w.get('page_count', '?')} | status={w.get('status', '?')}"
                    )
                return "\n".join(lines)

            if tool_name == "memory_tencentdb_wiki_search":
                query = (args.get("query") or "").strip()
                if not query:
                    return json.dumps({"error": "Missing required parameter: query"})
                limit = _coerce_limit(args.get("limit"))
                knowledge_id = (args.get("knowledge_id") or "").strip()

                if knowledge_id:
                    targets = [knowledge_id]
                    names = {knowledge_id: ""}
                else:
                    libs = self._wiki_libraries(client)
                    if not libs:
                        return "No knowledge libraries are available."
                    targets = [w.get("wiki_id") for w in libs[:_MAX_WIKI_FANOUT] if w.get("wiki_id")]
                    names = {w.get("wiki_id"): w.get("name", "") for w in libs}

                hits = []
                errors = []
                for kid in targets:
                    if not kid:
                        continue
                    try:
                        raw = client.call_tool(
                            kid, "search", {"query": query, "limit": limit}
                        )
                        results = (raw.get("data") or {}).get("results") or []
                        for r in results:
                            hits.append((r.get("score", 0), kid, names.get(kid, ""), r))
                    except Exception as e:
                        errors.append(f"{kid}: {e}")

                if not hits:
                    if errors:
                        return json.dumps({"error": "Wiki search failed", "detail": errors})
                    return f"No wiki pages found for '{query}'."

                hits.sort(key=lambda t: t[0], reverse=True)
                lines = []
                for score, kid, name, r in hits:
                    label = f"{name} ({kid})" if name else kid
                    lines.append(
                        f"- [{label}] {r.get('title', '')} (score {score:.2f})\n"
                        f"  path: {r.get('path', '')}\n"
                        f"  {r.get('snippet', '').strip()}"
                    )
                return "\n".join(lines)

            if tool_name == "memory_tencentdb_wiki_read":
                knowledge_id = (args.get("knowledge_id") or "").strip()
                path = (args.get("path") or "").strip()
                if not knowledge_id:
                    return json.dumps({"error": "Missing required parameter: knowledge_id"})
                if not path:
                    return json.dumps({"error": "Missing required parameter: path"})
                raw = client.call_tool(knowledge_id, "read_page", {"refs": [path]})
                items = (raw.get("data") or {}).get("items") or []
                if not items:
                    return f"Page '{path}' is empty or not found in {knowledge_id}."
                return "\n\n".join(item.get("content", "") for item in items)

            return json.dumps({"error": f"Unknown tool: {tool_name}"})

        except Exception as e:
            logger.warning("memory-tencentdb wiki tool %s failed: %s", tool_name, e)
            return json.dumps({"error": f"Wiki tool call failed: {e}"})

    # -- Optional hooks -------------------------------------------------------

    def on_memory_write(self, action: str, target: str, content: str,
                        metadata: Optional[Dict[str, Any]] = None) -> None:
        """Mirror built-in memory writes to memory-tencentdb for indexing.

        Hermes' built-in ``memory`` tool writes only to the local MEMORY.md / USER.md
        files; there is no automatic bridge to this provider.  This hook closes that
        gap so a preference learned in one session is retrievable from TDAI in the next.

        Behaviour:
          * add / replace / patch -> write an L0 conversation turn; the v3 pipeline
            extracts it into L1 asynchronously (~45s).
          * remove -> NOT mirrored.  The SDK exposes no atomic-delete endpoint, and
            writing a "this was retracted" turn would rely on the L1 model to
            supersede the old atom, which is unreliable.  Skipping is the safe
            behaviour: a stale memory is less harmful than a wrong one.

        Never raises: a failed mirror must not break the ``memory`` tool call.
        """
        try:
            if not self._client or not self._ensure_alive_for_request():
                return
            action = (action or "").lower()
            if action not in ("add", "replace", "patch"):
                logger.debug("memory-tencentdb: skip mirror for action=%s", action)
                return
            text = (content or "").strip()
            if not text:
                return

            ts = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            label = "偏好" if target == "user" else "环境事实"
            messages = [
                {"role": "user",
                 "content": f"请记住这条{label}约定：{text}",
                 "timestamp": ts},
                {"role": "assistant",
                 "content": f"已记住：{text}",
                 "timestamp": ts},
            ]
            self._client.conversation_add(
                messages,
                session_id=f"hermes-memory-{target}",
                team_id=self._team_id,
                agent_id=self._agent_id,
                user_id=self._user_id,
            )
            logger.info("memory-tencentdb: mirrored %s -> TDAI (target=%s)", action, target)
        except Exception as e:
            logger.warning("memory-tencentdb: mirror failed (%s): %s", action, e)

    def on_session_end(self, messages: List[Dict[str, Any]]) -> None:
        """Trigger session-level flush on the Gateway.

        v3: pipeline auto-handles session end via timer-based scanning.
        The explicit POST /session/end is no longer needed; kept as no-op.
        """
        # if self._client and self._gateway_available:
        #     try:
        #         self._client.end_session(
        #             session_key=self._session_id,
        #             user_id=self._user_id,
        #         )
        #     except Exception as e:
        #         logger.debug("memory-tencentdb on_session_end failed: %s", e)
        logger.debug(
            "memory-tencentdb on_session_end: v3 pipeline auto-handles, no-op "
            "(session=%s)", self._session_id,
        )

    # -- Config ---------------------------------------------------------------

    def get_config_schema(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": "gateway_cmd",
                "description": "Command to start the memory-tencentdb Gateway (e.g. 'node --import tsx /path/to/server.ts')",
                "env_var": "MEMORY_TENCENTDB_GATEWAY_CMD",
                "required": False,
            },
            {
                "key": "knowledge_host",
                "description": (
                    "MemoryKnowledge (wiki service) host — serves the shared "
                    "knowledge base searched by memory_tencentdb_wiki_search."
                ),
                "default": "127.0.0.1",
                "env_var": "MEMORY_TENCENTDB_KNOWLEDGE_HOST",
                "required": False,
            },
            {
                "key": "knowledge_port",
                "description": "MemoryKnowledge (wiki service) port.",
                "default": "8421",
                "env_var": "MEMORY_TENCENTDB_KNOWLEDGE_PORT",
                "required": False,
            },
            {
                "key": "knowledge_team",
                "description": (
                    "Team whose knowledge libraries are listed. Reads are not "
                    "team-scoped — every profile sees the same corpus."
                ),
                "default": "default",
                "env_var": "MEMORY_TENCENTDB_KNOWLEDGE_TEAM",
                "required": False,
            },
            {
                "key": "gateway_host",
                "description": "Gateway host",
                "default": "127.0.0.1",
                "env_var": "MEMORY_TENCENTDB_GATEWAY_HOST",
            },
            {
                "key": "gateway_port",
                "description": "Gateway port",
                "default": "8420",
                "env_var": "MEMORY_TENCENTDB_GATEWAY_PORT",
            },
            {
                "key": "gateway_api_key",
                "description": (
                    "Optional Bearer token attached to outbound Gateway "
                    "requests. Set this to the same secret you configure on "
                    "the Gateway side (``TDAI_GATEWAY_API_KEY`` / "
                    "``server.apiKey``) so the Bearer comparison succeeds. "
                    "Leave unset to skip the Authorization header entirely "
                    "(legacy default; matches an open Gateway)."
                ),
                "secret": True,
                "required": False,
                "env_var": "MEMORY_TENCENTDB_GATEWAY_API_KEY",
            },
            {
                "key": "llm_api_key",
                "description": "LLM API key (for Gateway's standalone LLM calls)",
                "secret": True,
                "required": True,
                "env_var": "MEMORY_TENCENTDB_LLM_API_KEY",
            },
            {
                "key": "llm_base_url",
                "description": "LLM API base URL",
                "default": "https://api.openai.com/v1",
                "env_var": "MEMORY_TENCENTDB_LLM_BASE_URL",
            },
            {
                "key": "llm_model",
                "description": "LLM model name",
                "default": "gpt-4o",
                "env_var": "MEMORY_TENCENTDB_LLM_MODEL",
            },
        ]


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------

def register(ctx) -> None:
    """Register memory-tencentdb as a memory provider plugin."""
    ctx.register_memory_provider(MemoryTencentdbProvider())
