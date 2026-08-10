"""Brightway PMF status inspection utilities.

This module contains helper functions for checking whether PMF-related methods,
flows, and exchanges have already been added to a Brightway project.
"""

from collections.abc import Iterable
from typing import Any

from .method import (
    VIACF_EXCHANGE_GROUPS,
    _find_relevant_databases,
    _get_viacf_exchange_coverage,
)

PMF_METHOD_ENDPOINT = "imaginaryendpoint"
PMF_METHOD_MIDPOINT = "imaginarymidpoint"


def _build_method_keys(method_names: Iterable[str]) -> tuple[tuple[str, str, str], ...]:
    # Brightway methods are identified by a 3-part tuple.
    return tuple(
        (method_name, PMF_METHOD_ENDPOINT, PMF_METHOD_MIDPOINT)
        for method_name in method_names
    )


# Markers that indicate whether each PMF implementation has already been added
# to a Brightway project.
PMF_IMPLEMENTATION_MARKERS = {
    "viacf": {
        "method_keys": _build_method_keys(
            (
                "PMF Biotic RMI",
                "PMF Biotic TMR",
                "Agrar RMI",
                "Agrar TMR",
                "Forest RMI",
                "Forest TMR",
                "Aqua RMI",
                "Aqua TMR",
                "PMF Abiotic RMI",
                "PMF Abiotic TMR",
                "Fossil RMI",
                "Fossil TMR",
                "Metal RMI",
                "Metal TMR",
                "Mineral RMI",
                "Mineral TMR",
            )
        ),
        "flow_names": ("Agrar RMI", "Agrar TMR", "Aquatic RMI", "Aquatic TMR"),
        "exchange_groups": VIACF_EXCHANGE_GROUPS,
    },
    "direct": {
        "method_keys": _build_method_keys(
            (
                "PMF (direct) Abiotic RMI",
                "PMF (direct) Abiotic TMR",
                "PMF (direct) Biotic RMI",
                "PMF (direct) Biotic TMR",
            )
        ),
        "flow_names": ("Overburden", "Biomass, used", "Biomass, unused"),
        "exchange_groups": tuple(
            {"exchange_name": name, "input_flow_name": name}
            for name in ("Overburden", "Biomass, used", "Biomass, unused")
        ),
    },
}


def _get_biosphere_flow_keys_by_name(
    biosphere_db, flow_names: Iterable[str]
) -> dict[str, tuple[str, str]]:
    # Build a name -> (database, code) mapping for the requested custom flows.
    wanted_flow_names = set(flow_names)
    flow_keys_by_name = {}

    for act in biosphere_db:
        act_name = act["name"]
        if act_name in wanted_flow_names and act_name not in flow_keys_by_name:
            flow_keys_by_name[act_name] = (act["database"], act["code"])

        if len(flow_keys_by_name) == len(wanted_flow_names):
            break

    return flow_keys_by_name


def _get_used_exchange_input_keys(
    ecoinvent_name: str,
    biosphere_name: str,
    flow_keys: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    # Query exchange rows directly instead of iterating over every activity and
    # materializing all exchanges through the ORM.
    if not flow_keys:
        return set()

    try:
        from bw2data.backends.schema import ExchangeDataset
    except ImportError:
        from bw2data.backends.peewee.schema import ExchangeDataset

    flow_codes = [code for _, code in flow_keys]
    query = (
        ExchangeDataset.select(ExchangeDataset.input_code)
        .where(
            ExchangeDataset.output_database == ecoinvent_name,
            ExchangeDataset.input_database == biosphere_name,
            ExchangeDataset.input_code << flow_codes,
            ExchangeDataset.type == "biosphere",
        )
        .distinct()
        .tuples()
    )

    return {(biosphere_name, input_code) for (input_code,) in query}


def get_pmf_implementation_status(
    bw_project_name: str | None = None,
    cf_version: str | None = None,
    ecoinvent_database: str | None = None,
    biosphere_database: str | None = None,
) -> dict[str, Any]:
    """Return the PMF implementation status for a Brightway project.

    Parameters
    ----------
    bw_project_name:
        Optional Brightway project name. If provided, the function switches to
        that project before inspecting its PMF-related state.
    cf_version:
        Optional characterization-factor version. When given, exchange groups
        backed by header-only CF tables are reported as unsupported instead of
        missing. If omitted, all legacy exchange groups remain required.
    ecoinvent_database, biosphere_database:
        Optional exact database names. Automatic detection is used only when
        each database role has a single matching candidate.

    Returns
    -------
    dict
        A dictionary describing whether the ``viacf`` and ``direct`` PMF
        implementations are present completely, partially, or not at all.
    """
    import bw2data as bd

    # Optionally switch to the requested project before inspecting its state.
    if bw_project_name is not None:
        bd.projects.set_current(bw_project_name)

    ecoinvent_name, biosphere_name = _find_relevant_databases(
        bd,
        ecoinvent_database=ecoinvent_database,
        biosphere_database=biosphere_database,
    )
    methods = set(bd.methods)

    biosphere_db = bd.Database(biosphere_name) if biosphere_name is not None else None
    implementation_status = {}
    flow_keys_by_name = {}
    used_exchange_input_keys = set()

    if biosphere_db is not None:
        all_required_flow_names = {
            flow_name
            for markers in PMF_IMPLEMENTATION_MARKERS.values()
            for flow_name in markers["flow_names"]
        }
        flow_keys_by_name = _get_biosphere_flow_keys_by_name(
            biosphere_db, all_required_flow_names
        )

    if ecoinvent_name is not None and biosphere_name is not None and flow_keys_by_name:
        used_exchange_input_keys = _get_used_exchange_input_keys(
            ecoinvent_name=ecoinvent_name,
            biosphere_name=biosphere_name,
            flow_keys=set(flow_keys_by_name.values()),
        )

    viacf_coverage = (
        _get_viacf_exchange_coverage(cf_version)
        if cf_version is not None
        else [{**group, "has_data": True} for group in VIACF_EXCHANGE_GROUPS]
    )

    for implementation_name, markers in PMF_IMPLEMENTATION_MARKERS.items():
        # A PMF implementation is considered complete only if the methods,
        # custom biosphere flows, and PMF-specific exchanges are all present.
        missing_method_keys = [
            method_key
            for method_key in markers["method_keys"]
            if method_key not in methods
        ]
        methods_complete = not missing_method_keys
        required_flow_names = markers["flow_names"]
        exchange_groups = (
            viacf_coverage
            if implementation_name == "viacf"
            else markers["exchange_groups"]
        )
        required_exchange_groups = [
            group for group in exchange_groups if group.get("has_data", True)
        ]
        unsupported_exchange_names = [
            group["exchange_name"]
            for group in exchange_groups
            if not group.get("has_data", True)
        ]
        missing_flow_names = [
            flow_name
            for flow_name in required_flow_names
            if flow_name not in flow_keys_by_name
        ]
        flows_complete = not missing_flow_names
        present_exchange_names = [
            group["exchange_name"]
            for group in exchange_groups
            if flow_keys_by_name.get(group["input_flow_name"])
            in used_exchange_input_keys
        ]
        required_exchange_names = [
            group["exchange_name"] for group in required_exchange_groups
        ]
        missing_exchange_names = [
            name
            for name in required_exchange_names
            if name not in present_exchange_names
        ]
        exchanges_complete = not missing_exchange_names
        implemented = methods_complete and flows_complete and exchanges_complete

        coverage_notes = []
        if cf_version is not None and implementation_name == "viacf":
            coverage_notes = [
                f"{group['coverage_label']} CF table for version {cf_version} "
                "contains no data rows."
                for group in exchange_groups
                if not group.get("has_data", True)
            ]

        implementation_status[implementation_name] = {
            "implemented": implemented,
            "partial": any(
                (methods_complete, flows_complete, bool(present_exchange_names))
            )
            and not implemented,
            "methods_complete": methods_complete,
            "flows_complete": flows_complete,
            "exchanges_complete": exchanges_complete,
            "missing_method_keys": missing_method_keys,
            "missing_flow_names": missing_flow_names,
            "required_exchange_names": required_exchange_names,
            "present_exchange_names": present_exchange_names,
            "missing_exchange_names": missing_exchange_names,
            "unsupported_exchange_names": unsupported_exchange_names,
            "coverage_notes": coverage_notes,
        }

    return {
        "project": bd.projects.current,
        "ecoinvent_database": ecoinvent_name,
        "biosphere_database": biosphere_name,
        "cf_version": cf_version,
        "viacf": implementation_status["viacf"],
        "direct": implementation_status["direct"],
        "any_pmf_implemented": any(
            implementation["implemented"]
            for implementation in implementation_status.values()
        ),
    }


def is_pmf_implemented(
    bw_project_name: str | None = None,
    cf_version: str | None = None,
    ecoinvent_database: str | None = None,
    biosphere_database: str | None = None,
) -> bool:
    """Return ``True`` if at least one PMF implementation is fully available.

    Parameters
    ----------
    bw_project_name:
        Optional Brightway project name to inspect.
    cf_version:
        Optional characterization-factor version used to determine coverage.
    ecoinvent_database, biosphere_database:
        Optional exact database names.
    """
    # Convenience wrapper for callers that only need a single boolean.
    status = get_pmf_implementation_status(
        bw_project_name=bw_project_name,
        cf_version=cf_version,
        ecoinvent_database=ecoinvent_database,
        biosphere_database=biosphere_database,
    )
    return status["any_pmf_implemented"]
