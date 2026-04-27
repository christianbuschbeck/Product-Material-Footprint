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
- looks for a database name containing `"cutoff"` and a database name containing `"biosphere"`
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
- the project contains an ecoinvent database with `"cutoff"` in its name
- the project contains a biosphere database with `"biosphere"` in its name

The function uses those names to identify the databases automatically.

## Usage

Example:

```python
import product_material_footprint.brightway.method as pmf

PRJ_NAME = "your_brightway_project"

pmf.create_pmf_method_viacf(
    cf_version="3.11",
    bw_project_name=PRJ_NAME,
)
```

## Supported CF versions

The repository currently includes these characterization factor sets:

- `3.9.1`
- `3.10`
- `3.10_EN15804`
- `3.11`

Pass one of these strings as `cf_version`.

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
- Re-running it in the same project may create duplicate exchanges for agricultural and aquatic processes.
- If no matching `"cutoff"` or `"biosphere"` database is found, the function raises a `ValueError`.
- The function prints unmatched flows and products to help with troubleshooting.
