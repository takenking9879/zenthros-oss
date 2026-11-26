# SPDX-License-Identifier: Apache-2.0

"""zenthros.spend(): what the Workspace's machines spend and the caps over
them; create(dry_run=True) names the cap that would hold a template's new
machine, and create says which one holds it (34.2)."""
import io
import json
from unittest import mock

import zenthros
from zenthros import cli, errors, machines, mcp, spending

from test_targets import Targets

SPEND = {"since": "2026-11-22T00:00:00Z", "todayCents": 250, "hourlyCents": 50, "machines": 1, "caps": [
    {"policy": "risk-hourly", "scope": "workspace", "limit": "estimatedHourlyCostCents", "hard": True,
     "max": 55, "used": 50, "left": 5},
    {"policy": "spot-two", "scope": "template aws-spot", "limit": "instances", "hard": True, "max": 2, "used": 1, "left": 1},
    {"policy": "gpu-one", "scope": "template gpu", "limit": "instances", "hard": True, "max": 1, "used": 1, "left": 0},
    {"policy": "hub-daily", "scope": "hub", "limit": "accumulatedDailyCostCents", "hard": False,
     "max": 100000, "used": 9000, "left": 90950},
]}


class SpendTest(Targets):
    def setUp(self):
        super().setUp()
        self.api.routes[("GET", "/v1/workspaces/risk/spend")] = (200, SPEND)

    def test_spend_says_today_now_and_each_cap(self):
        found = zenthros.spend()
        self.assertEqual((found.today, found.hourly, found.machines), (2.5, 0.5, 1))
        text = str(found)
        self.assertIn("today (since 2026-11-22 00:00 UTC): $2.50; now $0.50/h on 1 machine", text)
        self.assertIn("risk-hourly (workspace): $0.50 of $0.55 per hour, $0.05/h left for a new machine", text)
        self.assertIn("spot-two (template aws-spot): 1 of 2 machines, 1 more", text)
        self.assertIn("hub-daily (hub): $90.00 of $1000.00 per day, $909.50/h left for a new machine (warns only)", text)

    def test_a_cap_that_holds_a_template_is_named_by_the_dry_run(self):
        # aws-spot costs $0.068/h: 7 cents, more than the 5 the Workspace's cap leaves.
        plan = machines.create("aws-spot", name="gpu-1", dry_run=True)
        self.assertFalse(plan.ok)
        self.assertEqual(plan.problems, ["cap risk-hourly (workspace, per hour) leaves $0.05/h and the machine costs $0.07/h"])
        self.assertIn("problem:   cap risk-hourly", repr(plan))
        self.assertEqual(self.changes(), ([], []))
        # Another template's cap is not this one's; a cap that only warns holds nothing.
        self.assertEqual(spending.Spend.from_api(SPEND).holds("aws-spot", 5), [])

    def test_no_price_under_a_budget_is_a_hold(self):
        found = spending.Spend.from_api(SPEND)
        self.assertEqual(found.holds("aws-spot", None),
                         ["cap risk-hourly (workspace, per hour) needs the machine's price, and the catalog has none"])
        self.assertEqual(found.holds("gpu", 0), ["cap gpu-one (template gpu, machines) allows 1 and 1 are up"])

    def test_without_the_right_to_read_spend_the_dry_run_says_so(self):
        self.api.routes[("GET", "/v1/workspaces/risk/spend")] = (403, {"kind": "Status", "status": "Failure", "code": 403,
                                                                         "reason": "Forbidden", "message": "get zenthrosspend is forbidden"})
        plan = machines.create("aws-spot", name="gpu-1", dry_run=True)
        self.assertTrue(plan.ok, plan.problems)
        self.assertIn("the caps were not checked", repr(plan))

    def test_the_cli_and_the_agents_tool_say_the_same(self):
        out = io.StringIO()
        cli.main(["spend", "--json"], out=out)
        answer = json.loads(out.getvalue())
        self.assertEqual((answer["today_usd"], answer["hourly_usd"], answer["caps"][0]["policy"]), (2.5, 0.5, "risk-hourly"))
        self.assertIn("spend", {tool["name"] for tool in mcp.TOOLS})
        self.assertEqual(mcp._call("spend", {}, "s"), answer)
        out = io.StringIO()
        cli.main(["spend"], out=out)
        self.assertIn("risk-hourly (workspace)", out.getvalue())


class HeldTest(Targets):
    def test_create_says_which_cap_holds_the_machine_and_makes_nothing(self):
        held = 'policy "risk-hourly" estimatedHourlyCostCents cap 55 exceeded by claim c (would be 57)'
        templates = {"templates": [{"name": "aws-spot", "limit": 2, "used": 0}], "machines": []}
        made = {"name": "gpu-1", "template": "aws-spot", "pool": "risk-gpu-1-a1"}
        self.api.routes[("GET", "/v1/workspaces/risk/machines")] = lambda _: (200, {**templates, "machines": [dict(made, held=held)]})
        self.api.routes[("POST", "/v1/workspaces/risk/machines")] = (201, made)
        self.api.routes[("DELETE", "/v1/workspaces/risk/machines/gpu-1")] = (200, made)
        with mock.patch.object(machines, "_sleep"):
            with self.assertRaises(errors.Refused) as caught:
                machines._create_from_template(machines._api(), "aws-spot", "gpu-1")
        self.assertIn(held, str(caught.exception))
        self.assertIn("zenthros.spend()", str(caught.exception))
        self.assertIn(("DELETE", "/v1/workspaces/risk/machines/gpu-1"), [(r["method"], r["path"]) for r in self.api.requests])
