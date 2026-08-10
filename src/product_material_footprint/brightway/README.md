# Brightway Submodule of PMF

This submodule creates Product Material Footprint (PMF) methods for a Brightway project.

At the moment, the main entry point is the characterization-factor-based workflow:

```python
import product_material_footprint.brightway.method as pmf

pmf.create_pmf_method_viacf(cf_version="3.11", bw_project_name=PRJ_NAME)
```

## What it does

`create_pmf_method_viacf`:

- switches to the Brightway project given in `bw_project_name`
- uses explicitly selected ecoinvent and biosphere databases, or uniquely detects them by name
- loads PMF characterization factors from `product_material_footprint/characterization_factors`
- creates PMF impact assessment methods in Brightway
- adds the required biotic biosphere flows and corresponding biosphere exchanges for agricultural and aquatic products

## Installation

Install the package together with the Brightway dependency.

Directly from GitHub, without cloning the repository first:

```bash
pip install "product-material-footprint[brightway] @ git+https://github.com/christianbuschbeck/Product-Material-Footprint.git"
```

From a local clone:

```bash
pip install -e '.[brightway]'
```

## Prerequisites

Before running the function, make sure that:

- Brightway is installed
- the target Brightway project already exists
- the project contains an ecoinvent database and a biosphere database

Pass the exact database names whenever possible. If they are omitted, the
integration looks for `"cutoff"` and `"biosphere"` in database names. Automatic
detection succeeds only if exactly one candidate exists for each role. Multiple
matching databases raise a `ValueError` listing the candidates; this prevents a
premise database from being selected accidentally.

## Usage

Example:

```python
import product_material_footprint.brightway.method as pmf
import product_material_footprint.brightway.status as pmf_status

PRJ_NAME = "your_brightway_project"

pmf.create_pmf_method_viacf(
    cf_version="3.11",
    bw_project_name=PRJ_NAME,
    ecoinvent_database="ecoinvent-3.11-cutoff",
    biosphere_database="biosphere3",
)

status = pmf_status.get_pmf_implementation_status(
    bw_project_name=PRJ_NAME,
    cf_version="3.11",
    ecoinvent_database="ecoinvent-3.11-cutoff",
    biosphere_database="biosphere3",
)
print(status["viacf"])
```

The database arguments are optional for backward compatibility. The status
function can also inspect the current project when `bw_project_name` is omitted.

## Supported CF versions

The repository currently includes these characterization factor sets:

- `3.9.1`
- `3.10`
- `3.10_EN15804`
- `3.11`

Pass one of these strings as `cf_version`.

The agricultural tables contain CF data in the currently shipped versions. The
aquatic tables are header-only and therefore provide no aquatic coverage. The
installer continues to register the Aqua methods and supporting flows for
backward compatibility, but it does not create Aqua exchanges without CF rows.

## Methods created

The function creates the following Brightway methods:

- `PMF Biotic RMI`
- `PMF Biotic TMR`
- `Agrar RMI`
- `Agrar TMR`
- `Forest RMI`
- `Forest TMR`
- `Aqua RMI`
- `Aqua TMR`
- `PMF Abiotic RMI`
- `PMF Abiotic TMR`
- `Fossil RMI`
- `Fossil TMR`
- `Metal RMI`
- `Metal TMR`
- `Mineral RMI`
- `Mineral TMR`

## Important notes

- The function modifies the Brightway project by creating methods, biosphere flows, and biosphere exchanges.
- Re-running the CF-based installer is idempotent for methods, exact-name supporting flows, and exchanges identified by the same output activity, input flow, and `biosphere` exchange type.
- If one matching exchange already exists, its amount and display name are updated from the selected CF table. If duplicates already exist, they are retained and a warning is emitted; the installer never deletes them automatically.
- If no matching `"cutoff"` or `"biosphere"` database is found, the function raises a `ValueError`.
- The function prints unmatched flows and products to help with troubleshooting.

## Installation status and CF coverage

Call `get_pmf_implementation_status` with the same `cf_version` used for
installation. Its main fields mean:

- `implemented=True`: all methods, supporting flows, and exchanges required by
  CF tables that contain data rows are present.
- `partial=True`: at least one complete component or relevant exchange exists,
  but a required component is missing.
- `required_exchange_names`, `present_exchange_names`, and
  `missing_exchange_names`: the concrete version-dependent exchange markers.
- `missing_method_keys` and `missing_flow_names`: other concrete incomplete
  installation markers.
- `unsupported_exchange_names` and `coverage_notes`: exchange groups whose CF
  tables contain no data rows.

When `cf_version` is omitted, the status check retains its legacy conservative
behavior and expects all exchange groups. A header-only table is not treated as
a table of zero-valued factors. For a covered group, a calculated impact of `0`
means the model and registered factors produced zero; `unsupported` means no CF
data is available and no impact conclusion can be drawn. In particular, the
absence of Aqua exchanges for version 3.11 is reported as missing coverage, not
as a measured zero aquatic impact.
