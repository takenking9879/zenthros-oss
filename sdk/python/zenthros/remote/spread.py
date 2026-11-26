# SPDX-License-Identifier: Apache-2.0

"""f.map(configs, on=[...], shared=...): a list of configurations, each a
dict of fn's keyword arguments and each one call, spread over several
destinations; and grid(), every combination of some values as such a list.

    configs = [{"windows": [5, 20]}, {"windows": [20, 60]}, {"windows": [5, 20, 60, 150]}]
    calls = variant.map(configs, on=["aws-spot", "mi-pc"], shared={"data_uri": datos})
    calls.result()          # in the order of configs
    calls.where             # where each config went
    train.map(zenthros.grid(learning_rate=[0.05, 0.1], leaves=[15, 31]), on="aws-spot")

Each task runs fn(**shared, **config); an item that is no dict is fn's one
argument, as zenthros.map has it. Several destinations: the configs go
first where targets() says there is room (AVAILABLE), in turn, then in
turn over all of them; one job per destination. This module travels with
every call: the runner reads the arguments with it.
"""

import itertools
import time

from zenthros import errors


def grid(**values):
    """Every combination of values, first name slowest, as a list of dicts:
    grid(a=[1, 2], b=["x"]) == [{"a": 1, "b": "x"}, {"a": 2, "b": "x"}]."""
    if not values:
        raise errors.ValidationError("grid needs name=[values, ...]: grid(learning_rate=[0.05, 0.1])")
    for name, options in values.items():
        if isinstance(options, (str, bytes)) or not isinstance(options, (list, tuple, range)):
            raise errors.ValidationError(f"grid {name}={options!r}: a list of the values to try")
    names = list(values)
    return [dict(zip(names, combo)) for combo in itertools.product(*(values[name] for name in names))]


def arguments(item, shared):
    """(args, kwargs) of one task: a config is keyword arguments over shared;
    anything else is the one argument."""
    if isinstance(item, dict):
        return (), {**shared, **item}
    return (item,), dict(shared)


def check(items, shared):
    """Refuse, before anything runs, a config that names a shared argument."""
    if not isinstance(shared, dict):
        raise errors.ValidationError(f"shared={shared!r}: a dict of the arguments every config gets")
    for index, item in enumerate(items):
        clash = sorted(set(item) & set(shared)) if isinstance(item, dict) else []
        if clash:
            raise errors.ValidationError(f"config {index} {item!r} and shared both give {clash}: one of them")


def split(count, capacities):
    """The destination of each of count configs: in turn over those with
    room left (capacities: [(name, free)]), then in turn over all."""
    room = {name: max(0, int(free or 0)) for name, free in capacities}
    names = [name for name, _ in capacities]
    out, turn, queued = [], 0, 0
    for _ in range(count):
        free = [i for i in range(len(names)) if room[names[(turn + i) % len(names)]] > 0]
        if free:
            name = names[(turn + free[0]) % len(names)]
            room[name] -= 1
            turn = (names.index(name) + 1) % len(names)
        else:
            name = names[queued % len(names)]
            queued += 1
        out.append(name)
    return out


def name_of(on):
    return str(getattr(on, "name", "") or getattr(on, "id", "") or on)


class MapCalls:
    """The jobs of one f.map over several destinations: result() in the
    order of configs, where each config went, and delete/cancel for all."""

    def __init__(self, configs, parts, name=None):
        # The map's name: its jobs are <name>-<k>, and zenthros.get(name)
        # brings it back from another session.
        self.name = name
        self.configs = list(configs)
        # [(destination, [config index, ...], RemoteCall)]
        self.parts = list(parts)
        self.where = [None] * len(self.configs)
        for where, indices, _ in self.parts:
            for index in indices:
                self.where[index] = where

    @property
    def calls(self):
        return [call for _, _, call in self.parts]

    def result(self, timeout=None, return_exceptions=False, stream_logs=True, cancel_on_interrupt=True):
        """The results in the order of configs. Every destination's job is
        waited for (and the machine made for it removed) before the first
        failed config raises, naming it and where it ran, unless
        return_exceptions puts each error in its place. timeout is for the
        whole map. Ctrl-C cancels every job, unless cancel_on_interrupt=False."""
        out = [None] * len(self.configs)
        failed = []
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            for where, indices, call in self.parts:
                left = None if deadline is None else max(0.0, deadline - time.monotonic())
                for local, (ok, value) in enumerate(call.outcomes(timeout=left, stream_logs=stream_logs,
                                                                  cancel_on_interrupt=cancel_on_interrupt)):
                    index = indices[local]
                    if not ok:
                        value = self._located(value, index, where)
                        failed.append((index, value))
                    out[index] = value
        except KeyboardInterrupt:
            for call in self.calls:
                call._on_interrupt(cancel_on_interrupt)
            raise
        if failed and not return_exceptions:
            raise min(failed, key=lambda item: item[0])[1]
        return out

    def _located(self, err, index, where):
        from zenthros.remote.call import RemoteError

        if not isinstance(err, RemoteError):
            return err
        located = RemoteError(f"config {index} {self.configs[index]!r} on {where}: {err}", remote_type=err.remote_type,
                              remote_traceback=err.remote_traceback, job=err.job)
        located.__cause__ = err.__cause__
        return located

    def cost(self):
        """Each job's Cost, in the order of destinations."""
        return [call.cost() for call in self.calls]

    def cancel(self):
        for call in self.calls:
            call.cancel()

    def delete(self):
        for call in self.calls:
            call.delete()

    def release(self):
        for call in self.calls:
            call.release()

    def _last_reports(self):
        """Each config's last report, by config index: a job's task i is
        its i-th config (producer task-i)."""
        from zenthros.remote.call import last_reports

        out = {}
        for _, indices, call in self.parts:
            try:
                last = last_reports(call.reports())
            except (errors.ZenthrosError, OSError):  # a table, not a check
                continue
            for local, index in enumerate(indices):
                if f"task-{local}" in last:
                    out[index] = last[f"task-{local}"]
        return out

    def reports(self):
        """One row per config: its fields, where it ran, and its last
        report's payload. A pandas DataFrame when pandas is installed, else
        a list of dicts."""
        last = self._last_reports()
        rows = []
        for index, config in enumerate(self.configs):
            row = {"config": index, "on": self.where[index]}
            row.update(config if isinstance(config, dict) else {"item": config})
            row.update(last[index].payload if index in last else {})
            rows.append(row)
        try:
            import pandas
        except ImportError:
            return rows
        return pandas.DataFrame(rows)

    def __repr__(self):
        from zenthros.remote.call import report_text

        phases = {}
        for _, _, call in self.parts:
            try:
                phases[call.name] = call.status() or "Pending"
            except (errors.ZenthrosError, OSError):  # a table, not a check
                phases[call.name] = "?"
        last = self._last_reports()
        # A map brought back by zenthros.get() lacks the configs of a job
        # already collected: those rows say so.
        jobs = {index: call.name for _, indices, call in self.parts for index in indices}
        rows = [(str(i), repr(c), self.where[i] or "", jobs.get(i, "-"), phases.get(jobs.get(i), "gone"),
                 report_text(last[i].payload, limit=60) if i in last else "")
                for i, c in enumerate(self.configs)]
        head = ("#", "config", "on", "job", "state", "report")
        widths = [max(len(r[k]) for r in rows + [head]) for k in range(len(head))]
        lines = ["  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip() for row in [head, *rows]]
        spread = ", ".join(f"{where} ({len(indices)})" for where, indices, _ in self.parts)
        named = f"{self.name}: " if self.name else ""
        return f"{named}{len(self.configs)} configs on {spread}\n" + "\n".join(lines)


class SpreadPlan:
    """f.map(..., dry_run=True): how many configs each destination would
    take, and its Plan; nothing is made."""

    def __init__(self, plans):
        # destination -> (Plan, configs it would take)
        self.plans = dict(plans)

    @property
    def counts(self):
        return {where: count for where, (_, count) in self.plans.items()}

    @property
    def ok(self):
        return all(plan.ok for plan, _ in self.plans.values())

    @property
    def problems(self):
        return [f"{where}: {p}" for where, (plan, _) in self.plans.items() for p in plan.problems]

    def __repr__(self):
        blocks = []
        for where, (plan, count) in self.plans.items():
            body = "\n".join("  " + line for line in repr(plan).splitlines())
            blocks.append(f"{where}: {count} config{'s' if count != 1 else ''}\n{body}")
        return "\n".join(blocks)
