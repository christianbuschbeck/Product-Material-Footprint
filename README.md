# Product-Material-Footprint
This repository contains code for implementing the Product Material Footprint (PMF) in ecoinvent. 
For a more detailed description of PMF see: 
  - https://doi.org/10.3390/resources8020061
  - https://doi.org/10.3390/resources11060056

Two different PMF methods are present:
  - Direct PMF calculation is based exclusively on ecoinvent. !! This method is still under peer review. !!
  - Characterization factors based PMF calculation (_viacf) uses characterization factors (folder "CFs") from: https://doi.org/10.48662/daks-51.2

## Folder Structure
  - "src/product_material_footprint" contains the installable Python package.
  - "src/product_material_footprint/characterization_factors" contains the PMF characterization factors, including raw files and version-specific adaptations for different ecoinvent releases.
  - "src/product_material_footprint/brightway" contains the Brightway implementation, including the characterization-factor-based PMF method creation and its README.
  - "src/product_material_footprint/openlca" contains the openLCA script for characterization-factor-based PMF calculation and a short usage README.
  - "comparison DPMF - CFPMF" contains scripts and example data for comparing the direct PMF and characterization-factor-based PMF approaches.
  - "notebooks" contains exploratory notebooks and examples.
