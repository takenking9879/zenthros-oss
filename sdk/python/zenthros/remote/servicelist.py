# SPDX-License-Identifier: Apache-2.0

"""zenthros.services(): what of the Workspace is serving now, and what it
costs, for whom forgot one. zenthros.get(name).cancel() stops one;
serve(stop_after="4h") has the controller stop it on its own.

    zenthros.services()
    name  state    url                    serving  stops in         cost
    api   Running  http://127.0.0.1:8000  1h 0m    3h 0m            $0.1200
"""

import time
from dataclasses import dataclass, field
from typing import Optional

from zenthros import errors
from zenthros.jobs.handle import TERMINAL_PHASES, endpoints_from_status, seconds_at
from zenthros.remote.options import job_client, settings


def _now():
    return time.time()


def _took(seconds):
    if seconds is None:
        return ""
    hours, rest = divmod(max(0, int(seconds)), 3600)
    return f"{hours}h {rest // 60}m" if hours else f"{rest // 60}m {rest % 60}s"


@dataclass
class ServiceInfo:
    """One Service that has not ended. serving_seconds counts from its
    first start; stops_in_seconds is None when only a cancel ends it; cost
    is None where it cannot be read (no zenthros-api, or it failed)."""

    name: str
    phase: str
    urls: list = field(default_factory=list)
    serving_seconds: Optional[int] = None
    stops_in_seconds: Optional[int] = None
    cost: object = None

    def row(self):
        money = ""
        if self.cost is not None and self.cost.cost is not None:
            money = f"${self.cost.cost:.4f}" if self.cost.currency in ("", "USD") else f"{self.cost.cost:.4f} {self.cost.currency}"
        stops = "until cancelled" if self.stops_in_seconds is None else _took(self.stops_in_seconds)
        return (self.name, self.phase, " ".join(self.urls), _took(self.serving_seconds), stops, money)


class ServiceList(list):
    """services()'s list, shown as a table."""

    def __repr__(self):
        if not self:
            return "no Service is running in this Workspace"
        head = ("name", "state", "url", "serving", "stops in", "cost")
        rows = [s.row() for s in self]
        widths = [max(len(r[k]) for r in rows + [head]) for k in range(len(head))]
        return "\n".join("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in [head, *rows])


def services():
    """The Workspace's Services that have not ended, with their URLs, how
    long each has served, when stop_after stops it, and its cost so far."""
    client = job_client(settings())
    now = _now()
    out = ServiceList()
    for obj in client.list_jobs():
        template = (((obj.get("spec") or {}).get("workload") or {}).get("template")) or {}
        service = template.get("service")
        status = obj.get("status") or {}
        phase = str(status.get("phase") or "Pending")
        if not service or phase in TERMINAL_PHASES:
            continue
        name = (obj.get("metadata") or {}).get("name", "")
        started = seconds_at(status.get("firstStartTime") or status.get("startTime"))
        serving = int(now - started) if started is not None else None
        stop_after = service.get("stopAfterSeconds")
        stops_in = None
        if stop_after:
            stops_in = max(0, int(stop_after) - (serving or 0))
        out.append(ServiceInfo(name=name, phase=phase, urls=[e.url for e in endpoints_from_status(status) if e.url],
                               serving_seconds=serving, stops_in_seconds=stops_in, cost=_cost(client, name)))
    return out


def _cost(client, name):
    from zenthros.remote.call import Cost

    try:
        raw = client.job_cost(name)
    except errors.ZenthrosError:  # a table, not a check
        return None
    # None: no zenthros-api, no price to show.
    return Cost.from_api(raw) if raw is not None else None
