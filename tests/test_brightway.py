"""Tests for the Brightway integration without licensed ecoinvent data."""

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from product_material_footprint.brightway import method, status


class FakeDataset(dict):
    @property
    def key(self):
        return self["database"], self["code"]

    def save(self):
        return None


class FakeExchange(dict):
    def save(self):
        return None


class FakeActivity(FakeDataset):
    def __init__(self, **data):
        super().__init__(data)
        self._exchanges = []

    def exchanges(self):
        return list(self._exchanges)

    def new_exchange(self, *, input, amount, type):
        exchange = FakeExchange(input=input.key, amount=amount, type=type)
        self._exchanges.append(exchange)
        return exchange


class FakeDatabase(list):
    def __init__(self, name, datasets=()):
        super().__init__(datasets)
        self.name = name

    def new_activity(self, **data):
        activity = FakeActivity(**data)
        self.append(activity)
        return activity

    def search(self, query):
        return [dataset for dataset in self if query in dataset["name"]]


class FakeMethod:
    def __init__(self, bd, key):
        self.bd = bd
        self.key = key

    def register(self):
        self.bd.methods.add(self.key)

    def write(self, factors):
        self.bd.method_data[self.key] = list(factors)


class FakeProjects:
    def __init__(self):
        self.current = "default"

    def set_current(self, name):
        self.current = name


class FakeBrightway(SimpleNamespace):
    def __init__(self, databases):
        super().__init__()
        self._database_objects = databases
        self.databases = list(databases)
        self.methods = set()
        self.method_data = {}
        self.projects = FakeProjects()

    def Database(self, name):
        return self._database_objects[name]

    def Method(self, key):
        return FakeMethod(self, key)


def make_installer_brightway():
    biosphere = FakeDatabase("biosphere3")
    activity = FakeActivity(
        database="ecoinvent-3.11-cutoff",
        code="alfalfa-production",
        name="alfalfa production",
        **{"reference product": "alfalfa-grass mixture, Swiss integrated production"},
    )
    ecoinvent = FakeDatabase("ecoinvent-3.11-cutoff", [activity])
    return FakeBrightway(
        {biosphere.name: biosphere, ecoinvent.name: ecoinvent}
    ), activity


def add_viacf_flows(biosphere):
    for name in ("Agrar RMI", "Agrar TMR", "Aquatic RMI", "Aquatic TMR"):
        biosphere.append(FakeActivity(database=biosphere.name, code=name, name=name))


class DatabaseSelectionTests(unittest.TestCase):
    def test_automatic_selection_rejects_multiple_cutoff_databases(self):
        bd = FakeBrightway(
            {
                "ecoinvent-3.11-cutoff": FakeDatabase("ecoinvent-3.11-cutoff"),
                "premise-cutoff-2050": FakeDatabase("premise-cutoff-2050"),
                "biosphere3": FakeDatabase("biosphere3"),
            }
        )

        with self.assertRaisesRegex(
            ValueError, "Multiple ecoinvent database candidates"
        ):
            method._find_relevant_databases(bd)

        selected = method._find_relevant_databases(
            bd,
            ecoinvent_database="ecoinvent-3.11-cutoff",
            biosphere_database="biosphere3",
        )
        self.assertEqual(selected, ("ecoinvent-3.11-cutoff", "biosphere3"))


class CoverageTests(unittest.TestCase):
    def test_empty_and_nonempty_cf_tables_determine_required_exchanges(self):
        coverage = {
            group["exchange_name"]: group
            for group in method._get_viacf_exchange_coverage("3.11")
        }

        self.assertTrue(coverage["Agrar RMI"]["has_data"])
        self.assertTrue(coverage["Agrar TMR"]["has_data"])
        self.assertFalse(coverage["Aqua RMI"]["has_data"])
        self.assertFalse(coverage["Aqua TMR"]["has_data"])


class InstallerTests(unittest.TestCase):
    def test_exchange_upsert_updates_one_match_and_preserves_duplicates(self):
        flow = FakeActivity(database="biosphere3", code="Agrar RMI", name="Agrar RMI")
        activity = FakeActivity(
            database="ecoinvent-cutoff", code="crop", name="crop production"
        )
        existing = activity.new_exchange(input=flow, amount=0.1, type="biosphere")
        existing["name"] = "old name"

        method._upsert_biosphere_exchange(activity, flow, 0.2, "Agrar RMI")
        self.assertEqual(len(activity.exchanges()), 1)
        self.assertEqual(existing["amount"], 0.2)
        self.assertEqual(existing["name"], "Agrar RMI")

        duplicate = activity.new_exchange(input=flow, amount=0.3, type="biosphere")
        duplicate["name"] = "Agrar RMI"
        with self.assertWarnsRegex(
            RuntimeWarning, "existing duplicates were not changed"
        ):
            method._upsert_biosphere_exchange(activity, flow, 0.4, "Agrar RMI")

        self.assertEqual(len(activity.exchanges()), 2)
        self.assertEqual(
            [exchange["amount"] for exchange in activity.exchanges()], [0.2, 0.3]
        )

    def test_second_install_does_not_duplicate_and_tmr_uses_tmr_table(self):
        bd, activity = make_installer_brightway()

        with patch.dict(sys.modules, {"bw2data": bd}), redirect_stdout(StringIO()):
            method.create_pmf_method_viacf(
                cf_version="3.11",
                bw_project_name="test-project",
                ecoinvent_database="ecoinvent-3.11-cutoff",
                biosphere_database="biosphere3",
            )
            first_method_count = len(bd.methods)
            first_flow_count = len(bd.Database("biosphere3"))
            first_exchange_count = len(activity.exchanges())

            method.create_pmf_method_viacf(
                cf_version="3.11",
                bw_project_name="test-project",
                ecoinvent_database="ecoinvent-3.11-cutoff",
                biosphere_database="biosphere3",
            )

        self.assertEqual(len(bd.methods), first_method_count)
        self.assertEqual(len(bd.Database("biosphere3")), first_flow_count)
        self.assertEqual(len(activity.exchanges()), first_exchange_count)
        self.assertEqual(first_method_count, 16)
        self.assertEqual(first_flow_count, 4)
        self.assertEqual(first_exchange_count, 2)

        amounts = {
            exchange["name"]: exchange["amount"] for exchange in activity.exchanges()
        }
        self.assertEqual(amounts["Agrar RMI"], 0.15)
        self.assertEqual(amounts["Agrar TMR"], 0.42)


class StatusTests(unittest.TestCase):
    def test_empty_project_is_not_partial(self):
        bd = FakeBrightway({})

        with patch.dict(sys.modules, {"bw2data": bd}):
            result = status.get_pmf_implementation_status(cf_version="3.11")

        self.assertFalse(result["viacf"]["implemented"])
        self.assertFalse(result["viacf"]["partial"])

    def test_311_installation_is_complete_without_aquatic_exchanges(self):
        biosphere = FakeDatabase("biosphere3")
        add_viacf_flows(biosphere)
        bd = FakeBrightway(
            {
                "ecoinvent-3.11-cutoff": FakeDatabase("ecoinvent-3.11-cutoff"),
                "biosphere3": biosphere,
            }
        )
        bd.methods.update(status.PMF_IMPLEMENTATION_MARKERS["viacf"]["method_keys"])
        used_keys = {("biosphere3", "Agrar RMI"), ("biosphere3", "Agrar TMR")}

        with (
            patch.dict(sys.modules, {"bw2data": bd}),
            patch.object(
                status, "_get_used_exchange_input_keys", return_value=used_keys
            ),
        ):
            result = status.get_pmf_implementation_status(
                cf_version="3.11",
                ecoinvent_database="ecoinvent-3.11-cutoff",
                biosphere_database="biosphere3",
            )["viacf"]

        self.assertTrue(result["implemented"])
        self.assertFalse(result["partial"])
        self.assertEqual(result["required_exchange_names"], ["Agrar RMI", "Agrar TMR"])
        self.assertEqual(result["present_exchange_names"], ["Agrar RMI", "Agrar TMR"])
        self.assertEqual(result["missing_exchange_names"], [])
        self.assertEqual(result["unsupported_exchange_names"], ["Aqua RMI", "Aqua TMR"])
        self.assertTrue(result["coverage_notes"])

    def test_missing_required_agrar_exchanges_is_partial(self):
        biosphere = FakeDatabase("biosphere3")
        add_viacf_flows(biosphere)
        bd = FakeBrightway(
            {
                "ecoinvent-3.11-cutoff": FakeDatabase("ecoinvent-3.11-cutoff"),
                "biosphere3": biosphere,
            }
        )
        bd.methods.update(status.PMF_IMPLEMENTATION_MARKERS["viacf"]["method_keys"])

        with (
            patch.dict(sys.modules, {"bw2data": bd}),
            patch.object(status, "_get_used_exchange_input_keys", return_value=set()),
        ):
            result = status.get_pmf_implementation_status(
                cf_version="3.11",
                ecoinvent_database="ecoinvent-3.11-cutoff",
                biosphere_database="biosphere3",
            )["viacf"]

        self.assertFalse(result["implemented"])
        self.assertTrue(result["partial"])
        self.assertEqual(result["missing_exchange_names"], ["Agrar RMI", "Agrar TMR"])


if __name__ == "__main__":
    unittest.main()
