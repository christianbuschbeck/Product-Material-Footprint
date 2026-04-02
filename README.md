This repository contains code for implementing the Product Material Footprint (PMF) in ecoinvent. 
For a more detailed description of PMF see: 
  - DOI:10.3390/resources8020061
  - https://doi.org/10.3390/resources11060056

Two different PMF methods are present:
  - Direct PMF calculation is based exclusively on ecoinvent. !! This method is still under peer review. !!
  - Characterization factors based PMF calculation (_viacf) uses characterization factors (folder "CFs") from: https://doi.org/10.48662/daks-51.2

Folder Structure:
  - "CFs" contains the characterization factors used for direct PMF caluclation. The factors are adapted to the respective ecoinvent version due to changes in elementary flow names.
  - "brightway" contains brightway scripts for direct and characterization factor based PMF calculation
  - "comparison DPMF-CHPMF" contains a script and example data for analyzing differences between the two approaches.
  - "openLCA" conatins a script for characterization factor based PMF calculation

