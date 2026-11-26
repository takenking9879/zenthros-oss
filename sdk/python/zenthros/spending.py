# SPDX-License-Identifier: Apache-2.0

"""What the Workspace's machines spend and the caps over them: zenthros.spend(),
`zenthros spend`, the agents' spend tool, and what create(dry_run=True)
checks a template's new machine against."""

from dataclasses import asdict, dataclass, field

# A cap's limit as the administrator's QuotaPolicy names it, and how it reads.
_LIMITS = {
    "estimatedHourlyCostCents": "per hour",
    "accumulatedDailyCostCents": "per day",
    "instances": "machines",
    "gpus": "GPUs",
    "vcpus": "vCPUs",
}
_MONEY = ("estimatedHourlyCostCents", "accumulatedDailyCostCents")


def usd(cents):
    return f"${cents / 100:.2f}"


@dataclass
class Cap:
    """One limit of a policy over the Workspace. used counts everyone the
    policy covers (a template's or the hub's cap is shared with other
    Workspaces); left is what it leaves a new machine: for a budget, the most
    it may cost per hour (a daily one counts the next hour of the machines
    up), for a count, how many more fit. A cap that is not hard only warns."""

    policy: str
    # What it covers: "workspace", "template NAME", "pool NAME",
    # "workload KIND" or "hub".
    scope: str
    limit: str
    hard: bool
    max: int
    used: int
    left: int

    @classmethod
    def from_api(cls, row):
        return cls(policy=row["policy"], scope=row["scope"], limit=row["limit"], hard=bool(row["hard"]),
                   max=int(row["max"]), used=int(row["used"]), left=int(row["left"]))

    @property
    def money(self):
        return self.limit in _MONEY

    def _value(self, value):
        return usd(value) if self.money else str(value)

    def __str__(self):
        unit = _LIMITS.get(self.limit, self.limit)
        left = f"{usd(self.left)}/h left for a new machine" if self.money else f"{self.left} more"
        return (f"{self.policy} ({self.scope}): {self._value(self.used)} of {self._value(self.max)} {unit}, {left}"
                + ("" if self.hard else " (warns only)"))

    def holds(self, hourly_cents, cpus=0, gpus=0):
        """Why this cap holds a new machine that costs hourly_cents per hour
        (None: the catalog has no price for it), with cpus and gpus; "" when
        it does not."""
        if not self.hard:
            return ""
        name = f"cap {self.policy} ({self.scope}, {_LIMITS.get(self.limit, self.limit)})"
        if self.money:
            if hourly_cents is None:
                return f"{name} needs the machine's price, and the catalog has none"
            if hourly_cents > self.left:
                return f"{name} leaves {usd(self.left)}/h and the machine costs {usd(hourly_cents)}/h"
        elif self.limit == "instances" and self.left < 1:
            return f"{name} allows {self.max} and {self.used} are up"
        elif (self.limit == "vcpus" and cpus > self.left) or (self.limit == "gpus" and gpus > self.left):
            return f"{name} leaves {self.left} and the machine has {cpus if self.limit == 'vcpus' else gpus}"
        return ""


@dataclass
class Spend:
    """What the Workspace's machines spent today (since UTC midnight,
    released ones too) and cost per hour now, and each cap over them."""

    since: str
    today_cents: int
    hourly_cents: int
    machines: int
    caps: list = field(default_factory=list)

    @classmethod
    def from_api(cls, answer):
        return cls(since=answer["since"], today_cents=int(answer["todayCents"]), hourly_cents=int(answer["hourlyCents"]),
                   machines=int(answer["machines"]), caps=[Cap.from_api(row) for row in answer["caps"]])

    @property
    def today(self):
        """Today's spend, in USD."""
        return self.today_cents / 100

    @property
    def hourly(self):
        """What the machines up cost per hour now, in USD."""
        return self.hourly_cents / 100

    def holds(self, template, hourly_cents, cpus=0, gpus=0):
        """Why a new machine of template would be held: one line per cap
        that covers it (the Workspace's, the hub's and the template's own)
        and holds it."""
        covers = ("workspace", "hub", f"template {template}")
        return [why for cap in self.caps if cap.scope in covers for why in [cap.holds(hourly_cents, cpus, gpus)] if why]

    def to_dict(self):
        return {"since": self.since, "today_usd": self.today, "hourly_usd": self.hourly, "machines": self.machines,
                "caps": [asdict(cap) for cap in self.caps]}

    def __str__(self):
        lines = [f"today (since {self.since[:10]} 00:00 UTC): {usd(self.today_cents)}; now {usd(self.hourly_cents)}/h "
                 f"on {self.machines} machine{'' if self.machines == 1 else 's'}"]
        lines += [f"  {cap}" for cap in self.caps] or ["  no cap of the administrator reaches this Workspace"]
        return "\n".join(lines)

    __repr__ = __str__


def spend():
    """What the Workspace's machines spent today and cost per hour now, and
    each cap over them with what it leaves a new machine (a Spend)."""
    from zenthros.local import _api

    return Spend.from_api(_api().spend())
