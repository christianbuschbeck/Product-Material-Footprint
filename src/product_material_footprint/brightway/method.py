"""Brightway PMF method creation utilities.

This module contains functions that add Product Material Footprint (PMF)
methods and the required supporting flows or exchanges to a Brightway project.
"""

import warnings
from importlib.resources import files
from pathlib import Path
from typing import Any

import pandas as pd

VIACF_EXCHANGE_GROUPS = (
    {
        "exchange_name": "Agrar RMI",
        "input_flow_name": "Agrar RMI",
        "filename": "agrar_rmi.csv",
        "value_column": "CF RMI agriculture",
        "coverage_label": "Agricultural RMI",
    },
    {
        "exchange_name": "Agrar TMR",
        "input_flow_name": "Agrar TMR",
        "filename": "agrar_tmr.csv",
        # The source TMR table itself uses this historical column heading.
        "value_column": "CF RMI agriculture",
        "coverage_label": "Agricultural TMR",
    },
    {
        "exchange_name": "Aqua RMI",
        "input_flow_name": "Aquatic RMI",
        "filename": "aquatic_rmi.csv",
        "value_column": "CF RMI aquatic",
        "coverage_label": "Aquatic RMI",
    },
    {
        "exchange_name": "Aqua TMR",
        "input_flow_name": "Aquatic TMR",
        "filename": "aquatic_tmr.csv",
        "value_column": "CF TMR aquatic",
        "coverage_label": "Aquatic TMR",
    },
)


def _select_database(
    database_names: list[str], explicit_name: str | None, marker: str, role: str
) -> str | None:
    """Resolve one database name, rejecting ambiguous automatic matches."""
    if explicit_name is not None:
        if explicit_name not in database_names:
            raise ValueError(
                f"The explicitly selected {role} database {explicit_name!r} does not exist. "
                f"Available databases: {database_names!r}."
            )
        return explicit_name

    candidates = sorted(name for name in database_names if marker in name.casefold())
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple {role} database candidates contain {marker!r}: {candidates!r}. "
            f"Pass {role}_database explicitly."
        )
    return candidates[0] if candidates else None


def _find_relevant_databases(
    bd,
    ecoinvent_database: str | None = None,
    biosphere_database: str | None = None,
) -> tuple[str | None, str | None]:
    """Return explicit or uniquely auto-detected ecoinvent and biosphere names."""
    database_names = list(bd.databases)
    ecoinvent_name = _select_database(
        database_names, ecoinvent_database, "cutoff", "ecoinvent"
    )
    biosphere_name = _select_database(
        database_names, biosphere_database, "biosphere", "biosphere"
    )
    return ecoinvent_name, biosphere_name


def _get_cf_directory(cf_version: str) -> Path:
    base = files("product_material_footprint.characterization_factors")
    cf_dir = Path(base.joinpath(f"characterization_factors_{cf_version}"))
    if not cf_dir.is_dir():
        raise ValueError(
            f"No characterization-factor data found for version {cf_version!r}."
        )
    return cf_dir


def _load_viacf_exchange_tables(
    cf_version: str,
) -> list[tuple[dict[str, str], pd.DataFrame]]:
    """Load the tables which drive agricultural and aquatic exchanges."""
    cf_dir = _get_cf_directory(cf_version)
    return [
        (group, pd.read_csv(cf_dir / group["filename"], sep=";", decimal=","))
        for group in VIACF_EXCHANGE_GROUPS
    ]


def _get_viacf_exchange_coverage(cf_version: str) -> list[dict[str, Any]]:
    """Describe which version-specific exchange tables contain data rows."""
    return [
        {
            **group,
            "row_count": len(table.index),
            "has_data": not table.empty,
        }
        for group, table in _load_viacf_exchange_tables(cf_version)
    ]


def _build_factor_map(table: pd.DataFrame, value_column: str) -> dict[str, Any]:
    """Map materials to factors while retaining the source table's first match."""
    unique_rows = table.drop_duplicates(subset="Material", keep="first")
    return dict(zip(unique_rows["Material"], unique_rows[value_column], strict=True))


def _ensure_biosphere_flow(biosphere_db, biosphere_name: str, flow_name: str):
    """Return one exact flow, creating it if absent and warning on duplicates."""
    matches = [flow for flow in biosphere_db if flow["name"] == flow_name]
    if len(matches) > 1:
        warnings.warn(
            f"Found {len(matches)} biosphere flows named {flow_name!r}; using the first and "
            "leaving existing duplicates unchanged.",
            RuntimeWarning,
            stacklevel=2,
        )
    if matches:
        return matches[0]

    flow = biosphere_db.new_activity(
        **{
            "categories": ("natural resource", "none"),
            "code": flow_name,
            "CAS number": None,
            "name": flow_name,
            "database": biosphere_name,
            "unit": "kilogram",
            "type": "natural resource",
        }
    )
    flow.save()
    return flow


def _write_method(
    bd, method_key: tuple[str, str, str], factors: list[tuple[Any, Any]]
) -> None:
    """Register a method once and replace its factors on subsequent runs."""
    method = bd.Method(method_key)
    if method_key not in bd.methods:
        method.register()
    method.write(factors)


def _dataset_key(dataset) -> tuple[str, str]:
    key = getattr(dataset, "key", None)
    if key is not None:
        return tuple(key)
    return dataset["database"], dataset["code"]


def _exchange_input_key(exchange) -> tuple[str, str] | None:
    input_key = exchange.get("input")
    if input_key is not None:
        return tuple(input_key)
    input_dataset = getattr(exchange, "input", None)
    return _dataset_key(input_dataset) if input_dataset is not None else None


def _upsert_biosphere_exchange(
    activity, input_flow, amount: float, exchange_name: str
) -> None:
    """Create or update one exchange without silently removing duplicates."""
    input_key = _dataset_key(input_flow)
    matches = [
        exchange
        for exchange in activity.exchanges()
        if exchange.get("type") == "biosphere"
        and _exchange_input_key(exchange) == input_key
    ]

    if len(matches) > 1:
        warnings.warn(
            f"Found {len(matches)} biosphere exchanges from activity "
            f"{_dataset_key(activity)!r} to {input_key!r}; existing duplicates were not changed.",
            RuntimeWarning,
            stacklevel=2,
        )
        return

    if matches:
        exchange = matches[0]
        changed = (
            exchange.get("amount") != amount or exchange.get("name") != exchange_name
        )
        if changed:
            exchange["amount"] = amount
            exchange["name"] = exchange_name
            exchange.save()
        return

    exchange = activity.new_exchange(input=input_flow, amount=amount, type="biosphere")
    exchange["name"] = exchange_name
    exchange.save()


def create_pmf_method_viacf(
    cf_version: str,
    bw_project_name: str,
    ecoinvent_database: str | None = None,
    biosphere_database: str | None = None,
) -> None:
    """Create the characterization-factor-based PMF methods in a Brightway project.

    Parameters
    ----------
    cf_version:
        Characterization factor version to load, for example ``"3.11"``.
    bw_project_name:
        Name of the Brightway project that should receive the PMF methods.
    ecoinvent_database, biosphere_database:
        Optional exact database names. If omitted, automatic detection succeeds
        only when one unambiguous matching database exists.

    Raises
    ------
    ValueError
        If no database containing ``"cutoff"`` or ``"biosphere"`` can be found
        in the selected Brightway project.
    """
    import bw2data as bd

    cf_dir = _get_cf_directory(cf_version)

    bd.projects.set_current(bw_project_name)

    ecoinvent_name, biosphere_name = _find_relevant_databases(
        bd,
        ecoinvent_database=ecoinvent_database,
        biosphere_database=biosphere_database,
    )

    if ecoinvent_name is None:
        raise ValueError("No database containing 'cutoff' was found.")
    if biosphere_name is None:
        raise ValueError("No database containing 'biosphere' was found.")

    biosphere_db = bd.Database(biosphere_name)
    ecoinvent_db = bd.Database(ecoinvent_name)

    ###############
    ## Biotic  ####
    ###############

    rmi_agrar_flow = _ensure_biosphere_flow(biosphere_db, biosphere_name, "Agrar RMI")
    tmr_agrar_flow = _ensure_biosphere_flow(biosphere_db, biosphere_name, "Agrar TMR")
    rmi_aqua_flow = _ensure_biosphere_flow(biosphere_db, biosphere_name, "Aquatic RMI")
    tmr_aqua_flow = _ensure_biosphere_flow(biosphere_db, biosphere_name, "Aquatic TMR")

    rmi_forest_df = pd.read_csv(cf_dir / "forest_rmi.csv", sep=";", decimal=",")
    tmr_forest_df = pd.read_csv(cf_dir / "forest_tmr.csv", sep=";", decimal=",")

    rmi_forest = []
    tmr_forest = []

    # Forestry
    for idx in range(rmi_forest_df.shape[0]):
        f = rmi_forest_df.at[idx, "Material"]
        v = rmi_forest_df.at[idx, "CF RMI forestry"]
        x = [act for act in biosphere_db if f in act["name"]]
        if x != []:
            rmi_forest.append((x[0], v))

    for idx in range(tmr_forest_df.shape[0]):
        f = tmr_forest_df.at[idx, "Material"]
        v = tmr_forest_df.at[idx, "CF TMR forestry"]
        x = [act for act in biosphere_db if f in act["name"]]
        if x != []:
            tmr_forest.append((x[0], v))

    # Agrar and Aqua
    rmi_agrar = []
    tmr_agrar = []
    rmi_aqua = []
    tmr_aqua = []

    rmi_agrar.append((rmi_agrar_flow, 1))
    tmr_agrar.append((tmr_agrar_flow, 1))
    rmi_aqua.append((rmi_aqua_flow, 1))
    tmr_aqua.append((tmr_aqua_flow, 1))

    rmi_biotic = rmi_agrar + rmi_forest + rmi_aqua
    tmr_biotic = tmr_agrar + tmr_forest + tmr_aqua

    # Biotic
    pmf_biotic_rmi_method_key = ("PMF Biotic RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, pmf_biotic_rmi_method_key, rmi_biotic)
    # bd.Method(pmf_biotic_rmi_method_key).load()

    pmf_biotic_tmr_method_key = ("PMF Biotic TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, pmf_biotic_tmr_method_key, tmr_biotic)
    # bd.Method(pmf_biotic_tmr_method_key).load()

    # Agrar
    agrar_rmi_method_key = ("Agrar RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, agrar_rmi_method_key, rmi_agrar)
    # bd.Method(agrar_rmi_method_key).load()

    agrar_tmr_method_key = ("Agrar TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, agrar_tmr_method_key, tmr_agrar)
    # bd.Method(agrar_tmr_method_key).load()

    # Forest
    forest_rmi_method_key = ("Forest RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, forest_rmi_method_key, rmi_forest)
    # bd.Method(forest_rmi_method_key).load()

    forest_tmr_method_key = ("Forest TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, forest_tmr_method_key, tmr_forest)
    # bd.Method(forest_tmr_method_key).load()

    # Aqua
    aqua_rmi_method_key = ("Aqua RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, aqua_rmi_method_key, rmi_aqua)
    # bd.Method(aqua_rmi_method_key).load()

    aqua_tmr_method_key = ("Aqua TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, aqua_tmr_method_key, tmr_aqua)
    # bd.Method(aqua_tmr_method_key).load()

    """
    cf_list = bd.Method(forest_rmi_method_key).load()
    sorted_cf_list = sorted(cf_list, key=lambda x: bd.get_activity(x[0])['name'].lower())

    # Gib die sortierte Liste aus
    for flow_key, cf_value in sorted_cf_list:
        flow = bd.get_activity(flow_key)
        print(flow['name'], '-', flow['unit'], '-', cf_value)
    """

    ################
    ## Abiotic  ####
    ################
    notfound = []

    ### Fossil
    rmi_fossil_df = pd.read_csv(cf_dir / "fossil_rmi.csv", sep=";", decimal=",")
    tmr_fossil_df = pd.read_csv(cf_dir / "fossil_tmr.csv", sep=";", decimal=",")

    rmi_fossil = []
    tmr_fossil = []

    for idx in range(rmi_fossil_df.shape[0]):
        f = rmi_fossil_df.at[idx, "Flow"]
        v = rmi_fossil_df.at[idx, "CF RMI fossil"]
        x = [act for act in biosphere_db if f in act["name"]]
        if x == []:
            print("not found: " + f)
        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    rmi_fossil.append((el, v))

    for idx in range(tmr_fossil_df.shape[0]):
        f = tmr_fossil_df.at[idx, "Flow"]
        v = tmr_fossil_df.at[idx, "CF TMR fossil"]
        x = [act for act in biosphere_db if f in act["name"]]
        if x == []:
            print("not found: " + f)
        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    tmr_fossil.append((el, v))

    ### Metals
    rmi_metal_df = pd.read_csv(cf_dir / "metal_rmi.csv", sep=";", decimal=",")
    tmr_metal_df = pd.read_csv(cf_dir / "metal_tmr.csv", sep=";", decimal=",")

    rmi_metal = []
    tmr_metal = []

    for idx in range(rmi_metal_df.shape[0]):
        f = rmi_metal_df.at[idx, "Material"]
        v = rmi_metal_df.at[idx, "CF RMI metal ores"]
        x = [act for act in biosphere_db if f in act["name"]]
        found = False

        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    found = True
                    rmi_metal.append((el, v))
                    break
        if found is False:
            notfound.append(f)

    for idx in range(tmr_metal_df.shape[0]):
        f = tmr_metal_df.at[idx, "Material"]
        v = tmr_metal_df.at[idx, "CF TMR metal ores"]
        x = [act for act in biosphere_db if f in act["name"]]
        found = False

        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    found = True
                    tmr_metal.append((el, v))
                    break
        if found is False:
            notfound.append(f)

    ### Minerals
    rmi_mineral_df = pd.read_csv(cf_dir / "mineral_rmi.csv", sep=";", decimal=",")
    tmr_mineral_df = pd.read_csv(cf_dir / "mineral_tmr.csv", sep=";", decimal=",")

    rmi_mineral = []
    tmr_mineral = []

    for idx in range(rmi_mineral_df.shape[0]):
        f = rmi_mineral_df.at[idx, "Material"]
        v = rmi_mineral_df.at[idx, "CF RMI non-metallic minerals"]
        x = [act for act in biosphere_db if f in act["name"]]
        found = False
        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    found = True
                    rmi_mineral.append((el, v))
                    break
        if found is False:
            notfound.append(f)

    for idx in range(tmr_mineral_df.shape[0]):
        f = tmr_mineral_df.at[idx, "Material"]
        v = tmr_mineral_df.at[idx, "CF TMR non-metallic minerals"]
        x = [act for act in biosphere_db if f in act["name"]]
        found = False
        if x != []:
            for el in x:
                if "natural resource" in el["categories"][0]:
                    found = True
                    tmr_mineral.append((el, v))
                    break
        if found is False:
            notfound.append(f)

    abiotic_rmi = rmi_fossil + rmi_metal + rmi_mineral
    abiotic_tmr = tmr_fossil + tmr_metal + tmr_mineral

    # Abiotic
    pmf_abiotic_rmi_method_key = ("PMF Abiotic RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, pmf_abiotic_rmi_method_key, abiotic_rmi)
    # bd.Method(pmf_abiotic_rmi_method_key).load()

    pmf_abiotic_tmr_method_key = ("PMF Abiotic TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, pmf_abiotic_tmr_method_key, abiotic_tmr)
    # bd.Method(pmf_abiotic_tmr_method_key).load()

    # Fossil
    fossil_rmi_method_key = ("Fossil RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, fossil_rmi_method_key, rmi_fossil)
    # bd.Method(fossil_rmi_method_key).load()

    fossil_tmr_method_key = ("Fossil TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, fossil_tmr_method_key, tmr_fossil)
    # bd.Method(fossil_tmr_method_key).load()

    # Metal
    metal_rmi_method_key = ("Metal RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, metal_rmi_method_key, rmi_metal)
    # bd.Method(metal_rmi_method_key).load()

    metal_tmr_method_key = ("Metal TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, metal_tmr_method_key, tmr_metal)
    # bd.Method(metal_tmr_method_key).load()

    # Mineral
    mineral_rmi_method_key = ("Mineral RMI", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, mineral_rmi_method_key, rmi_mineral)
    # bd.Method(mineral_rmi_method_key).load()

    mineral_tmr_method_key = ("Mineral TMR", "imaginaryendpoint", "imaginarymidpoint")
    _write_method(bd, mineral_tmr_method_key, tmr_mineral)
    # bd.Method(mineral_tmr_method_key).load()

    print(notfound)

    #########################
    ## Biotic Add Flows  ####
    #########################

    exchange_tables = _load_viacf_exchange_tables(cf_version)
    flows_by_name = {
        "Agrar RMI": rmi_agrar_flow,
        "Agrar TMR": tmr_agrar_flow,
        "Aquatic RMI": rmi_aqua_flow,
        "Aquatic TMR": tmr_aqua_flow,
    }
    factor_maps = {
        group["exchange_name"]: _build_factor_map(table, group["value_column"])
        for group, table in exchange_tables
    }
    found_products = {group["exchange_name"]: set() for group, _ in exchange_tables}

    for act in ecoinvent_db:
        reference_product = act.get("reference product")
        if "production" not in act["name"]:
            continue

        for group, _ in exchange_tables:
            factors = factor_maps[group["exchange_name"]]
            if reference_product not in factors:
                continue

            found_products[group["exchange_name"]].add(reference_product)
            _upsert_biosphere_exchange(
                activity=act,
                input_flow=flows_by_name[group["input_flow_name"]],
                amount=float(factors[reference_product]),
                exchange_name=group["exchange_name"],
            )

    tables_by_exchange_name = {
        group["exchange_name"]: table for group, table in exchange_tables
    }
    not_found_agrar = [
        item
        for item in tables_by_exchange_name["Agrar RMI"]["Material"]
        if item not in found_products["Agrar RMI"]
    ]
    not_found_aqua = [
        item
        for item in tables_by_exchange_name["Aqua RMI"]["Material"]
        if item not in found_products["Aqua RMI"]
    ]

    print(not_found_agrar)
    print(not_found_aqua)

    # Test PMF Method
    """
    proc_name         = "steel production, electric, chromium steel 18/8"
    reference_product = "steel, chromium steel 18/8"
    location          = "RER"

    wanted_act = [act for act in bd.Database(ecoinvent_name) if proc_name in act['name']
                  and reference_product.strip() in act['reference product']
                  and location in act['location']][0]

    rmi_abiotic_key = [m for m in bd.methods if "PMF Abiotic RMI" in str(m)][0]

    fu = {wanted_act:1}

    lca = bc.LCA(fu,rmi_abiotic_key)
    lca.lci()
    lca.lcia()
    LCA_result = lca.score

    print("abiotic RMI for " + wanted_act["name"] + "  :  " + str(LCA_result) + " kg")
    """

    """
    for flow_key, cf in bd.Method(abiotic_rmi_method_key).load():
        flow = bd.get_activity(id=flow_key)
        print(
            flow["name"],
            "| categories:", flow["categories"],
            "| unit:", flow["unit"],
            "| CF:", cf
        )
    """


def create_pmf_method_direct(
    bw_project_name: str | None = None,
    ecoinvent_database: str | None = None,
    biosphere_database: str | None = None,
) -> None:
  """Create the direct PMF methods and supporting exchanges in Brightway.

  This workflow derives PMF information directly from the available ecoinvent
  and biosphere data in the current Brightway project.

  Parameters
  ----------
  bw_project_name:
      Optional Brightway project name. If omitted, the current project is used.
  ecoinvent_database, biosphere_database:
      Optional exact database names. Automatic detection succeeds only when
      one unambiguous matching database exists for each role.
  """
  import bw2data as bd

  if bw_project_name is not None:
    bd.projects.set_current(bw_project_name)

  ecoinvent_name, biosphere_name = _find_relevant_databases(
      bd,
      ecoinvent_database=ecoinvent_database,
      biosphere_database=biosphere_database,
  )
  if ecoinvent_name is None:
    raise ValueError("No database containing 'cutoff' was found.")
  if biosphere_name is None:
    raise ValueError("No database containing 'biosphere' was found.")

  ################################
  ## Additional biosphere flows ##
  ################################

  if len(bd.Database(biosphere_name).search("Overburden"))==0:
    overburden = bd.Database(biosphere_name).new_activity(**{
        'categories': ('natural resource', 'in ground'),
        'code'      : "overburden",
        'CAS number': None,
        'name'      : 'Overburden',
        'database'  : biosphere_name,
        'unit'      : 'kilogram',
        'type'      : 'natural resource'})
    overburden.save()


  if len(bd.Database(biosphere_name).search("Biomass, used"))==0:
    biomass_used = bd.Database(biosphere_name).new_activity(**{
        'categories': ('natural resource', 'none'),
        'code'      : "biomass, used",
        'CAS number': None,
        'name'      : 'Biomass, used',
        'database'  : biosphere_name,
        'unit'      : 'kilogram',
        'type'      : 'natural resource'})
    biomass_used.save()


  if len(bd.Database(biosphere_name).search("Biomass, unused"))==0:
    biomass_unused = bd.Database(biosphere_name).new_activity(**{
        'categories': ('natural resource', 'none'),
        'code'      : "biomass, unused",
        'CAS number': None,
        'name'      : 'Biomass, unused',
        'database'  : biosphere_name,
        'unit'      : 'kilogram',
        'type'      : 'natural resource'})
    biomass_unused.save()




  overburden     = bd.Database(biosphere_name).search("Overburden")[0]
  biomass_used   = bd.Database(biosphere_name).search("Biomass, used")[0]
  biomass_unused = bd.Database(biosphere_name).search("Biomass, unused")[0]

  ############################
  ### O V E R B U R D E N  ###
  ############################

  for act in bd.Database(ecoinvent_name):
    amount = 0
    needstobein = False
    alreadyin   = False
    if "market" not in act["name"] and "treatment" not in act["name"]:

      for ex in act.exchanges():
        if ex["name"] == "Overburden":
          alreadyin = True
        if ex["name"] == "non-sulfidic overburden, off-site" or ex["name"] == "spoil from hard coal mining" or ex["name"] == "spoil from lignite mining":
          amount = amount + abs(ex["amount"])
          needstobein = True

      if needstobein == True and alreadyin == True:

        for ex in act.exchanges():
          if ex["name"] == "Overburden":
            ex["amount"] = amount
            ex.save()


      if needstobein == True and alreadyin == False:
        wanted_act = act
        new_exc = wanted_act.new_exchange(input= overburden, amount=amount, type = "biosphere")
        new_exc["name"] = "Overburden"
        new_exc.save()

  #################################
  ### B I O T I C    U S E D    ###
  #################################

  agriculture_categories_list = [
    "0111:Growing of cereals (except rice), leguminous crops and oil seeds",
    "0112:Growing of rice",
    "0113:Growing of vegetables and melons, roots and tubers",
    "0114:Growing of sugar cane",
    "0116:Growing of fibre crops",
    "0119:Growing of other non-perennial crops",

    "0121:Growing of grapes",
    "0122:Growing of tropical and subtropical fruits",
    "0123:Growing of citrus fruits",
    "0124:Growing of pome fruits and stone fruits",
    "0125:Growing of other tree and bush fruits and nuts",
    "0126:Growing of oleaginous fruits",
    "0127:Growing of beverage crops",
    "0128:Growing of spices, aromatic, drug and pharmaceutical crops",
    "0129:Growing of other perennial crops",
  ]

  forestry_categories_list = [
    "0210:Silviculture and other forestry activities",
    "0220:Logging"
  ]

  ### Agriculture ###


  ### Fishing ###

  for act in bd.Database(ecoinvent_name):
    skip        = True
    biomass_amount = 0

    for ex in act.exchanges():
      if "Fish," in ex["name"]:
        skip = False
        biomass_amount = biomass_amount + ex.amount

    if skip == False:
      wanted_act = act
      new_exc = wanted_act.new_exchange(input= biomass_used, amount=biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, used"
      new_exc.save()


  ### Rest ###

  for act in bd.Database(ecoinvent_name):
    skip = True
    mass_per_energy = 1 / 19.5
    for ex in act.exchanges():
      if ex["name"] =="Energy, gross calorific value, in biomass":
        skip = False
        biomass_amount = mass_per_energy * ex.amount

    for ex in act.exchanges():
      if ex["name"] == "Biomass, used":
        skip = True

    if skip == False:
      wanted_act = act
      new_exc = wanted_act.new_exchange(input= biomass_used, amount=biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, used"
      new_exc.save()

  #####################################
  ### B I O T I C    U N U S E D    ###
  #####################################

  def mean_function(x):
    return sum(x)/len(x)

  ### Agriculture ###
  mean_moisture_content_resdidue = 0.5

  for act in bd.Database(ecoinvent_name):
    skip = True
    if "market" not in act["name"]:
      for cat in agriculture_categories_list:
        if cat in act["classifications"][0] and act["unit"] == "kilogram":
          skip = False

    for ex in act.exchanges():
      if ex["name"] == "Biomass, unused":
        skip = True

    if skip == False:
      for ex in act.exchanges():
        residue_ratio = 1

        if "wheat" in act["name"]:
          residue_ratio = mean_function([1.3,1.2,1.34,1.75,0.6,1,
                                        1.7,1.7,1.6,0.8,1.7,
                                        1.3,1.3,1.5,0.9,1.3])
        if "barley" in act["name"]:
          residue_ratio = mean_function([1.3,1.5,1,1.75,1,1.24,1.2,1])
        if "rye" in act["name"]:
          residue_ratio = mean_function([1.75,1.7])
        if "maize" in act["name"]:
          residue_ratio = mean_function([1,1,0.9,2,1.3,1,0.7,1,1,1])
        if "sunflower" in act["name"]:
          residue_ratio = mean_function([1.5,2.6,1.4,])
        if "rape" in act["name"]:
          residue_ratio = mean_function([1.1,1.7,1.7])
        if "rice" in act["name"]:
          residue_ratio = mean_function([1.76,1])

        unused_biomass_amount = 1 * residue_ratio * (1- mean_moisture_content_resdidue)

      wanted_act = act

      new_exc = wanted_act.new_exchange(input= biomass_unused, amount=unused_biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, unused"
      new_exc.save()
      #print(wanted_act)

  ### Forestry ###

  for act in bd.Database(ecoinvent_name):
    skip = True
    if "market" not in act["name"]:
      for cat in forestry_categories_list:
        if cat in act["classifications"][0]:
          for ex in act.exchanges():
            if ex["name"] == "Biomass, used":
              skip = False
              used_biomass_amount = ex.amount
              residue_ratio = 4 / 6

    for ex in act.exchanges():
      if ex["name"] == "Biomass, unused":
        skip = True

    if skip == False:
      unused_biomass_amount = used_biomass_amount * residue_ratio

      wanted_act = act
      new_exc = wanted_act.new_exchange(input= biomass_unused, amount=unused_biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, unused"
      new_exc.save()
      #print(wanted_act)

  ### Animal ###
  act = wanted_act
  for act in bd.Database(ecoinvent_name):
    skip = True
    if "market" not in act["name"]:
      for cat in forestry_categories_list:
        if cat in act["classifications"][0]:
          for ex in act.exchanges():
            if ex["name"] == "Biomass, used":
              skip = False
              used_biomass_amount = ex.amount
              residue_ratio = 0

    for ex in act.exchanges():
      if ex["name"] == "Biomass, unused":
        skip = True

    if skip == False:
      unused_biomass_amount = used_biomass_amount * residue_ratio

      wanted_act = act
      new_exc = wanted_act.new_exchange(input= biomass_unused, amount=unused_biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, unused"
      new_exc.save()
      #print(wanted_act)

  ### Fishing ###


  for act in bd.Database(ecoinvent_name):
    skip = True
    if (
      "market" not in act["name"]
      and act["classifications"][0] == "0311:Marine fishing"
    ):
      for ex in act.exchanges():
        if ex["name"] == "Biomass, used":
          skip = False
          used_biomass_amount = ex.amount
          residue_ratio = 4 / 6

    for ex in act.exchanges():
      if ex["name"] == "Biomass, unused":
        skip = True

    if skip == False:
      for ex in act.exchanges():
        if ex["name"] == "Biomass, used":
          used_biomass_amount = ex.amount

      unused_biomass_amount = used_biomass_amount * residue_ratio

      wanted_act = act
      new_exc = wanted_act.new_exchange(input= biomass_unused, amount=unused_biomass_amount, type = "biosphere")
      new_exc["name"] = "Biomass, unused"
      new_exc.save()
      #print(wanted_act)

  ##################
  ## MI Method  ####
  ##################

  abiotic_rmi = []
  abiotic_tmr = []
  biotic_rmi  = []
  biotic_tmr  = []

  for x in bd.Database(biosphere_name):

    # abiotic resources
    if x['categories'][0] == 'natural resource' and x['categories'][1] == 'in ground' and x["unit"] == "kilogram" and "Overburden" not in x['name']:
      abiotic_rmi.append((x,1))
      abiotic_tmr.append((x,1))
    if x['categories'][0] == 'natural resource' and x['categories'][1] == 'in ground' and x["unit"] == "standard cubic meter":
      abiotic_rmi.append((x,0.8))
      abiotic_tmr.append((x,0.8))


    if x['name'] == "Overburden" :
      abiotic_tmr.append((x,1))


    # biotic resources
    if x["name"] == "Biomass, used":
      biotic_rmi.append((x,1))
      biotic_tmr.append((x,1))

    if x["name"] == "Biomass, unused":
      biotic_tmr.append((x,1))


  mi_abiotic_rmi_method_key = ('PMF (direct) Abiotic RMI', 'imaginaryendpoint', 'imaginarymidpoint')
  bd.Method(mi_abiotic_rmi_method_key).register()
  bd.Method(mi_abiotic_rmi_method_key).write(abiotic_rmi)
  bd.Method(mi_abiotic_rmi_method_key).load()

  mi_abiotic_tmr_method_key = ('PMF (direct) Abiotic TMR', 'imaginaryendpoint', 'imaginarymidpoint')
  bd.Method(mi_abiotic_tmr_method_key).register()
  bd.Method(mi_abiotic_tmr_method_key).write(abiotic_tmr)
  bd.Method(mi_abiotic_tmr_method_key).load()

  mi_biotic_rmi_method_key = ('PMF (direct) Biotic RMI', 'imaginaryendpoint', 'imaginarymidpoint')
  bd.Method(mi_biotic_rmi_method_key).register()
  bd.Method(mi_biotic_rmi_method_key).write(biotic_rmi)
  bd.Method(mi_biotic_rmi_method_key).load()

  mi_biotic_tmr_method_key = ('PMF (direct) Biotic TMR', 'imaginaryendpoint', 'imaginarymidpoint')
  bd.Method(mi_biotic_tmr_method_key).register()
  bd.Method(mi_biotic_tmr_method_key).write(biotic_tmr)
  bd.Method(mi_biotic_tmr_method_key).load()

  # # Test PMF Method
  """
  proc_name         = "steel production, electric, chromium steel 18/8"
  reference_product = "steel, chromium steel 18/8"
  location          = "RER"

  wanted_act = [act for act in bd.Database(ecoinvent_name) if proc_name in act['name']
                and reference_product.strip() in act['reference product']
                and location in act['location']][0]

  rmi_abiotic_key = [m for m in bd.methods if "PMF (direct) Abiotic RMI" in str(m)][0]

  fu = {wanted_act:1}

  lca = bc.LCA(fu,rmi_abiotic_key)
  lca.lci()
  lca.lcia()
  LCA_result = lca.score

  print("abiotic RMI for " + wanted_act["name"] + "  :  " + str(LCA_result) + " kg")
  """
