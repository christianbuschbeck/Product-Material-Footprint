"""Brightway PMF status inspection utilities.

This module contains helper functions for checking whether PMF-related methods,
flows, and exchanges have already been added to a Brightway project.
"""

from .method import _find_relevant_databases


PMF_METHOD_ENDPOINT = "imaginaryendpoint"
PMF_METHOD_MIDPOINT = "imaginarymidpoint"


def _build_method_keys(method_names):
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
        "exchange_names": ("Agrar RMI", "Agrar TMR", "Aqua RMI", "Aqua TMR"),
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
        "exchange_names": ("Overburden", "Biomass, used", "Biomass, unused"),
    },
}


def _has_named_biosphere_flows(biosphere_db, flow_names):
    # All required custom biosphere flows must be present.
    found_flow_names = {act["name"] for act in biosphere_db if act["name"] in flow_names}
    return all(flow_name in found_flow_names for flow_name in flow_names)


def _has_named_exchanges(ecoinvent_db, exchange_names):
    # Scan activities until all required PMF-related exchanges have been found.
    found_exchange_names = set()

    for act in ecoinvent_db:
        for exc in act.exchanges():
            exc_name = exc.get("name")
            if exc_name in exchange_names:
                found_exchange_names.add(exc_name)

        if len(found_exchange_names) == len(exchange_names):
            return True

    return all(exchange_name in found_exchange_names for exchange_name in exchange_names)


def get_pmf_implementation_status(bw_project_name: str | None = None):
    """Return the PMF implementation status for a Brightway project.

    Parameters
    ----------
    bw_project_name:
        Optional Brightway project name. If provided, the function switches to
        that project before inspecting its PMF-related state.

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

    ecoinvent_name, biosphere_name = _find_relevant_databases(bd)
    methods = set(bd.methods)

    biosphere_db = bd.Database(biosphere_name) if biosphere_name is not None else None
    ecoinvent_db = bd.Database(ecoinvent_name) if ecoinvent_name is not None else None
    implementation_status = {}

    for implementation_name, markers in PMF_IMPLEMENTATION_MARKERS.items():
        # A PMF implementation is considered complete only if the methods,
        # custom biosphere flows, and PMF-specific exchanges are all present.
        methods_complete = all(method_key in methods for method_key in markers["method_keys"])
        flows_complete = (
            _has_named_biosphere_flows(biosphere_db, markers["flow_names"])
            if biosphere_db is not None
            else False
        )
        exchanges_complete = (
            _has_named_exchanges(ecoinvent_db, markers["exchange_names"])
            if ecoinvent_db is not None
            else False
        )
        implemented = methods_complete and flows_complete and exchanges_complete

        implementation_status[implementation_name] = {
            "implemented": implemented,
            "partial": any((methods_complete, flows_complete, exchanges_complete)) and not implemented,
            "methods_complete": methods_complete,
            "flows_complete": flows_complete,
            "exchanges_complete": exchanges_complete,
        }

    return {
        "project": bd.projects.current,
        "ecoinvent_database": ecoinvent_name,
        "biosphere_database": biosphere_name,
        "viacf": implementation_status["viacf"],
        "direct": implementation_status["direct"],
        "any_pmf_implemented": any(
            implementation["implemented"] for implementation in implementation_status.values()
        ),
    }


def is_pmf_implemented(bw_project_name: str | None = None):
    """Return ``True`` if at least one PMF implementation is fully available.

    Parameters
    ----------
    bw_project_name:
        Optional Brightway project name to inspect.
    """
    # Convenience wrapper for callers that only need a single boolean.
    status = get_pmf_implementation_status(bw_project_name=bw_project_name)
    return status["any_pmf_implemented"]
