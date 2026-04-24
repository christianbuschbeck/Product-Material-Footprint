###################################################################################

# This script creates a new LCIA-Method for the PMF. It is an indput based
# indicator that describes the mass of material which is extracted in order to
# produce a certain product or fulfill a certain service.

# !! It is important to note, that for the LCIA-Method to work, the script has to be run
# at least once for each database. Exporting and Importing the LCIA-Method leads to wrong
# results because the required elementary flows are not created. !!

##################################### 1. SETUP #####################################

####################################################################
###       C H A R A C T E R I Z A T I O N    F A C T O R S       ###
####################################################################

# The directory of the files containing the characterization factors needs to be set
#! Be careful to choose the CFs matching to ecoinvent in the currently opened database
#! If flows are not found (e.g. because it is the wrong version), you will be informed
cf_dir =''
#!!! Be aware that this script deletes all unused flows !!!



######################################
###        M O D U L E S           ###
######################################

# The required Modules are imported

from org.openlca.jsonld.input import JsonImport
from org.openlca.jsonld import JsonStoreReader
from org.openlca.jsonld import ZipStore
from org.openlca.app.db import Database
from org.openlca.core.database import Derby
from java.io import File
import org.openlca.core.model as model
from org.openlca.core.database import UnitGroupDao, FlowPropertyDao
from java.util import UUID
from org.openlca.app.util import UI
from org.openlca.app import App
from org.openlca.core.database import ProcessDao
from org.openlca.core.database import FlowDao
from org.openlca.core.database import CategoryDao
from org.openlca.core.database import FlowPropertyDao
import csv
import json
import os
import shutil


######################################
###      D I R E C T O R Y         ###
######################################

# The directory where openLCA stores its databases needs to be found,
# because the LCIA-Method is later stored in that directory

def search_folder(start_path, target_folder,count = 0):
  for root, dirs, files in os.walk(start_path):
    # Check if the target folder exists in the current directory
    if target_folder in dirs:
      target_path = os.path.join(root, target_folder)  # Full path to the target folder
      # Check if the target folder contains a subfolder named 'database'
      subdirs = os.listdir(target_path)  # Get the list of items in the target folder
      if 'databases' in subdirs and "edv" not in target_path:  # Check if 'database' exists in the target folder
        return target_path
  return None

start_path    = '/Users'
target_folder = 'openLCA-data-1.4'

mainpath      = search_folder(start_path, target_folder)

############################
### F U N C T I O N S    ###
############################

# The function CF_generate will later fill in characterization factors into .json-files, which make up the LCIA-Method

def read_CF(dateiname):
  name_list = []
  val_list  = []

  with open(dateiname, 'r') as f:
    zeilen = f.readlines()
    # Erste Zeile sind die Spaltennamen
    spalten = zeilen[0].strip().split('\t')
    for zeile in zeilen[1:]:
      werte = zeile.strip().split('\t')
      name_list.append(werte[0].split(";")[0])

      val_str   = werte[0].split(";")[3]
      val_list.append(float(val_str.replace(',', '.')))

    return [name_list,val_list]

def CF_generate(mli, Val, dnames, duuid, dunit, dcatpath):
    # Create dictionary with characterisation factors

    # Define the unit for mass
    U_Mass = {
        "@type": "Unit",
        "@id": "20aadc24-a391-41cf-b340-3e4529f44bde",
        "name": "kg"
    }

    # Define the unit for energy
    U_Energy = {
        "@type": "Unit",
        "@id": "52765a6c-3896-43c2-b2f4-c679acf13efe",
        "name": "MJ"
    }

    # Define the unit for volume
    U_Volume = {
        "@type": "Unit",
        "@id": "1c3a9695-398d-4b1f-b07e-a8715b610f70",
        "name": "m3"
    }

    # Define the flow property for mass
    FP_Mass = {
        "@type": "FlowProperty",
        "@id": "93a60a56-a3c8-11da-a746-0800200b9a66",
        "name": "Mass",
        "categoryPath": ["Technical flow properties"]
    }

    # Define the flow property for energy
    FP_Energy = {
        "@type": "FlowProperty",
        "@id": "f6811440-ee37-11de-8a39-0800200c9a66",
        "name": "Energy",
        "categoryPath": ["Technical flow properties"]
    }

    # Define the flow property for volume
    FP_Volumne = {
        "@type": "FlowProperty",
        "@id": "93a60a56-a3c8-22da-a746-0800200c9a66",
        "name": "Volume",
        "categoryPath": ["Technical flow properties"]
    }

    # Initialize the characterization factor dictionary
    CF = {}
    CF["@type"] = "ImpactFactor"
    CF["value"] = Val[mli]

    # Set the flow details in the characterization factor dictionary
    CF["flow"] = {
        "@type": "Flow",
        "@id": duuid[mli],
        "name": dnames[mli],
        "categoryPath": dcatpath,
        "flowType": "ELEMENTARY_FLOW",
        "refUnit": dunit[mli]
    }

    # Assign the appropriate unit and flow property based on the unit type
    if dunit[mli] == 'kg':
        CF["unit"] = U_Mass
        CF["flowProperty"] = FP_Mass
    elif dunit[mli] == 'MJ':
        CF["unit"] = U_Energy
        CF["flowProperty"] = FP_Energy
    elif dunit[mli] == 'm3':
        CF["unit"] = U_Volume
        CF["flowProperty"] = FP_Volumne
    else:
        None

    # Return the completed characterization factor dictionary
    return CF



#######################################
###  D B   C O N N E C T I O N      ###
#######################################

# The connection to the open Database is established
db         = Database.get()
ei_version = db.name


# Dao objects are set. Those are used to iterate over the respective Model Type (e.g. Processes or Flows etc.)
dao_fp = FlowPropertyDao(db)
dao_c  = CategoryDao(db)
dao_p  = ProcessDao(db)
dao_f  = FlowDao(db)
dao_u  = UnitGroupDao(db)
dao_m  = ImpactMethodDao(db)
dao_i  = ImpactCategoryDao(db)

allflows      = dao_f.getAll()
allprocesses  = dao_p.getAll()
allmethods    = dao_m.getAll()
allimpcat     = dao_i.getAll()
allcategories = dao_c.getAll()
allunits      = dao_u.getAll()
allproperties = dao_fp.getAll()

allflows_base = []
for f in allflows:
  if "name" in dir(f.category):
    allflows_base.append(f)

###################################################
###  D e l e t e  e x i s t i n g   M e t h o d  ##
###################################################

for meth in allmethods:
  if meth.name == "PMF":
    dao_m.delete(meth)

for ic in allimpcat:
  if ic.category.name == "PMF":
    dao_i.delete(ic)

for c in allcategories:

  if c.name == "PMF":
    dao_c.delete(c)


##############################################
###  D e l e t e  u n u s e d   F l o w s  ###
##############################################
DEL = True

if DEL == True:

  # Create a set to store used flow IDs
  used_flow_ids = set()

  # Loop through all processes to find used flows
  for proc in allprocesses:
      for ex in proc.exchanges:
          used_flow_ids.add(ex.flow.id)

  # Loop through all flows and delete if not used
  deleted = 0
  for flow in allflows_base:
      if flow.id not in used_flow_ids:
          #print("Deleting unused flow:", flow.name)
          db.delete(flow)
          deleted += 1

  print("Done. Deleted", deleted, "unused flows.")

######################################################################
###  N E C E S S A R Y    U N I T S    A N D   P R O P E R T I E S ###
######################################################################

# Mass (in kg) is the only used unit in this LCIA-Method

for u in allunits:
  if u.name =="Units of mass":
    for uu in u.units:
    	if uu.name== "kg":
          kg = uu

for prop in allproperties:
  if prop.name =="Mass":
    mass = prop

#######################################
###  N E C E S S A R Y    F L O W S ###
#######################################

# All necessary flows are set. A random flow from the biotic resources category serves as template for
# flows that are introduced and should be stored in the category "Resource/biotic".

allflow_names = []

for f in allflows_base:
  allflow_names.append(f.name)
  if f.category.name == "biotic" and f.category.category.name == "Resource" and f.referenceUnit.name == "kg":
    elem_flow_biotic = f

# The "Agrar RMI" and "Agrar TMR" elementary flows are created (if not already present), because for agricultural products,
# the characterization factors refer to a reference product - not an elementary flow.
# Hence, respective elementary flows need to be added
print(dao_f.getForName("Agrar RMI"))
if "Agrar RMI" in allflow_names:
  agrar_rmi_flow = dao_f.getForName("Agrar RMI")[0]
else:
  agrar_rmi_flow       = elem_flow_biotic.copy()
  agrar_rmi_flow.name  = 'Agrar RMI'
  agrar_rmi_flow.refId = "8711a380-e9dc-4bbf-be2b-91d243a8e39d"
  dao_f.insert(agrar_rmi_flow)


if "Agrar TMR" in allflow_names:
  agrar_tmr_flow = dao_f.getForName("Agrar TMR")[0]
else:
  agrar_tmr_flow       = elem_flow_biotic.copy()
  agrar_tmr_flow.name  = 'Agrar TMR'
  agrar_tmr_flow.refId = "9442f771-1473-40d6-8dab-8ffbb94fec1d"
  dao_f.insert(agrar_tmr_flow)



##################################### 2. Create list with CF   #####################################

###############################################################################
### C R E A T E    L I S T S    F O R    I M P A C T   C A T E G O R I E S  ###
###############################################################################

# For creating the LCIA Method several lists need to be created and filled.
# The lists contain the following information:
# 1) The uuid of the elementary flow
# 2) The name of the elementary flow
# 3) The category of the elementary flow
# 4) The unit of the elementary flow
# 5) The value of the characterization factor

allflows      = dao_f.getAll() # Updated with newly added flows

for f in allflows:
  if "name" in dir(f.category):
    allflows_base.append(f)

### Fossil RMI
found = []

fossil_rmi_names  = read_CF(cf_dir + "/fossil_rmi.csv")[0]
fossil_rmi_values = read_CF(cf_dir + "/fossil_rmi.csv")[1]

fossil_rmi_uuid    = [0.0] * len(fossil_rmi_names)
fossil_rmi_catpath = [0.0] * len(fossil_rmi_names)
fossil_rmi_units   = ["kg"]* len(fossil_rmi_names)


for f in allflows_base:
  if f.category.name in ["in ground", "biotic"] :
    if f.name in fossil_rmi_names:
      idx = [i for i, name in enumerate(fossil_rmi_names) if name == f.name][0]
      fossil_rmi_uuid[idx] = f.refId
      fossil_rmi_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in fossil_rmi_names if item not in found]
if len(not_found) > 0:
  print("Fossil RMI not found:")
  print(not_found)

fossil_rmi_dict ={
    "names"  : fossil_rmi_names,
    "uuids"  : fossil_rmi_uuid,
    "units"  : fossil_rmi_units,
    "values" : fossil_rmi_values,
    "catpath": fossil_rmi_catpath}


### Fossil TMR
found = []

fossil_tmr_names  = read_CF(cf_dir + "/fossil_tmr.csv")[0]
fossil_tmr_values = read_CF(cf_dir + "/fossil_tmr.csv")[1]

fossil_tmr_uuid    = [0.0] * len(fossil_tmr_names)
fossil_tmr_catpath = [0.0] * len(fossil_tmr_names)
fossil_tmr_units   = ["kg"]* len(fossil_tmr_names)

for f in allflows_base:
  if f.category.name in ["in ground", "biotic"] :
    if f.name in fossil_tmr_names:
      idx = [i for i, name in enumerate(fossil_tmr_names) if name == f.name][0]
      fossil_tmr_uuid[idx] = f.refId
      fossil_tmr_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in fossil_tmr_names if item not in found]
if len(not_found) > 0:
  print("Fossil TMR not found:")
  print(not_found)

fossil_tmr_dict ={
    "names"  : fossil_tmr_names,
    "uuids"  : fossil_tmr_uuid,
    "units"  : fossil_tmr_units,
    "values" : fossil_tmr_values,
    "catpath": fossil_tmr_catpath}


### Metal RMI
found = []

metal_rmi_names  = read_CF(cf_dir + "/metal_rmi.csv")[0]
metal_rmi_values = read_CF(cf_dir + "/metal_rmi.csv")[1]

metal_rmi_uuid    = [0.0] * len(metal_rmi_names)
metal_rmi_catpath = [0.0] * len(metal_rmi_names)
metal_rmi_units   = ["kg"]* len(metal_rmi_names)

for f in allflows_base:
  if f.category.name == "in ground":
    if f.name in metal_rmi_names:
      idx = [i for i, name in enumerate(metal_rmi_names) if name == f.name][0]
      metal_rmi_uuid[idx] = f.refId
      metal_rmi_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in metal_rmi_names if item not in found]
if len(not_found) > 0:
  print("Metal RMI not found:")
  print(not_found)


metal_rmi_dict ={
    "names"  : metal_rmi_names,
    "uuids"  : metal_rmi_uuid,
    "units"  : metal_rmi_units,
    "values" : metal_rmi_values,
    "catpath": metal_rmi_catpath}



### Metal TMR
found = []

metal_tmr_names  = read_CF(cf_dir + "/metal_tmr.csv")[0]
metal_tmr_values = read_CF(cf_dir + "/metal_tmr.csv")[1]

metal_tmr_uuid    = [0.0] * len(metal_tmr_names)
metal_tmr_catpath = [0.0] * len(metal_tmr_names)
metal_tmr_units   = ["kg"]* len(metal_tmr_names)

for f in allflows_base:
  if f.category.name == "in ground":
    if f.name in metal_tmr_names:
      idx = [i for i, name in enumerate(metal_tmr_names) if name == f.name][0]
      metal_tmr_uuid[idx] = f.refId
      metal_tmr_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in metal_tmr_names if item not in found]
if len(not_found) > 0:
  print("Metal TMR not found:")
  print(not_found)

metal_tmr_dict ={
    "names"  : metal_tmr_names,
    "uuids"  : metal_tmr_uuid,
    "units"  : metal_tmr_units,
    "values" : metal_tmr_values,
    "catpath": metal_tmr_catpath}


### Mineral RMI
found = []

found = []
mineral_rmi_names  = read_CF(cf_dir + "/mineral_rmi.csv")[0]
mineral_rmi_values = read_CF(cf_dir + "/mineral_rmi.csv")[1]

mineral_rmi_uuid    = [0.0] * len(mineral_rmi_names)
mineral_rmi_catpath = [0.0] * len(mineral_rmi_names)
mineral_rmi_units   = ["kg"]* len(mineral_rmi_names)

for f in allflows_base:
  if f.category.name == "in ground":
    if f.name in mineral_rmi_names:
      idx = [i for i, name in enumerate(mineral_rmi_names) if name == f.name][0]
      mineral_rmi_uuid[idx] = f.refId
      mineral_rmi_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in mineral_rmi_names if item not in found]
if len(not_found) > 0:
  print("Mineral RMI not found:")
  print(not_found)


mineral_rmi_dict ={
    "names"  : mineral_rmi_names,
    "uuids"  : mineral_rmi_uuid,
    "units"  : mineral_rmi_units,
    "values" : mineral_rmi_values,
    "catpath": mineral_rmi_catpath}

### Mineral TMR
found = []

mineral_tmr_names  = read_CF(cf_dir + "/mineral_tmr.csv")[0]
mineral_tmr_values = read_CF(cf_dir + "/mineral_tmr.csv")[1]

mineral_tmr_uuid    = [0.0] * len(mineral_tmr_names)
mineral_tmr_catpath = [0.0] * len(mineral_tmr_names)
mineral_tmr_units   = ["kg"]* len(mineral_tmr_names)

for f in allflows_base:
  if f.category.name == "in ground":
    if f.name in mineral_tmr_names:
      idx = [i for i, name in enumerate(mineral_tmr_names) if name == f.name][0]
      mineral_tmr_uuid[idx] = f.refId
      mineral_tmr_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in mineral_tmr_names if item not in found]
if len(not_found) > 0:
  print("Mineral TMR not found:")
  print(not_found)

mineral_tmr_dict ={
    "names"  : mineral_tmr_names,
    "uuids"  : mineral_tmr_uuid,
    "units"  : mineral_tmr_units,
    "values" : mineral_tmr_values,
    "catpath": mineral_tmr_catpath}


### Abiotic ###

abiotic_rmi_dict ={
    "names"  : fossil_rmi_names   + metal_rmi_names   + mineral_rmi_names,
    "uuids"  : fossil_rmi_uuid    + metal_rmi_uuid    + mineral_rmi_uuid,
    "units"  : fossil_rmi_units   + metal_rmi_units   + mineral_rmi_units,
    "values" : fossil_rmi_values  + metal_rmi_values  + mineral_rmi_values,
    "catpath": fossil_rmi_catpath + metal_rmi_catpath + mineral_rmi_catpath}

abiotic_tmr_dict ={
    "names"  : fossil_tmr_names   + metal_tmr_names   + mineral_tmr_names,
    "uuids"  : fossil_tmr_uuid    + metal_tmr_uuid    + mineral_tmr_uuid,
    "units"  : fossil_tmr_units   + metal_tmr_units   + mineral_tmr_units,
    "values" : fossil_tmr_values  + metal_tmr_values  + mineral_tmr_values,
    "catpath": fossil_tmr_catpath + metal_tmr_catpath + mineral_tmr_catpath}



### Agrar RMI
found = []
agrar_rmi_names  = ["Agrar RMI"]
agrar_rmi_values = [1]

agrar_rmi_uuid    = [0.0] * len(agrar_rmi_names)
agrar_rmi_catpath = [0.0] * len(agrar_rmi_names)
agrar_rmi_units   = ["kg"]* len(agrar_rmi_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in agrar_rmi_names:
      idx = [i for i, name in enumerate(agrar_rmi_names) if name == f.name][0]
      agrar_rmi_uuid[idx] = f.refId
      agrar_rmi_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in agrar_rmi_names if item not in found]
if len(not_found) > 0:
  print("Agrar RMI not found:")
  print(not_found)

agrar_rmi_dict ={
    "names"  : agrar_rmi_names,
    "uuids"  : agrar_rmi_uuid,
    "units"  : agrar_rmi_units,
    "values" : agrar_rmi_values,
    "catpath": agrar_rmi_catpath}


### Agrar TMR
found = []

agrar_tmr_names  = ["Agrar TMR"]
agrar_tmr_values = [1]

agrar_tmr_uuid    = [0.0] * len(agrar_tmr_names)
agrar_tmr_catpath = [0.0] * len(agrar_tmr_names)
agrar_tmr_units   = ["kg"]* len(agrar_tmr_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in agrar_tmr_names:
      idx = [i for i, name in enumerate(agrar_tmr_names) if name == f.name][0]
      agrar_tmr_uuid[idx] = f.refId
      agrar_tmr_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in agrar_tmr_names if item not in found]
if len(not_found) > 0:
  print("Agrar TMR not found:")
  print(not_found)

agrar_tmr_dict ={
    "names"  : agrar_tmr_names,
    "uuids"  : agrar_tmr_uuid,
    "units"  : agrar_tmr_units,
    "values" : agrar_tmr_values,
    "catpath": agrar_tmr_catpath}



### Forest RMI
found = []
forest_rmi_names  = read_CF(cf_dir + "/forest_rmi.csv")[0]
forest_rmi_values = read_CF(cf_dir + "/forest_rmi.csv")[1]

forest_rmi_uuid    = [0.0] * len(forest_rmi_names)
forest_rmi_catpath = [0.0] * len(forest_rmi_names)
forest_rmi_units   = ["kg"]* len(forest_rmi_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in forest_rmi_names:
      idx = [i for i, name in enumerate(forest_rmi_names) if name == f.name][0]
      forest_rmi_uuid[idx] = f.refId
      forest_rmi_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in forest_rmi_names if item not in found]
if len(not_found) > 0:
  print("Forest RMI not found:")
  print(not_found)


forest_rmi_dict ={
    "names"  : forest_rmi_names,
    "uuids"  : forest_rmi_uuid,
    "units"  : forest_rmi_units,
    "values" : forest_rmi_values,
    "catpath": forest_rmi_catpath}

### Forest TMR
found = []

forest_tmr_names  = read_CF(cf_dir + "/forest_tmr.csv")[0]
forest_tmr_values = read_CF(cf_dir + "/forest_tmr.csv")[1]

forest_tmr_uuid    = [0.0] * len(forest_tmr_names)
forest_tmr_catpath = [0.0] * len(forest_tmr_names)
forest_tmr_units   = ["kg"]* len(forest_tmr_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in forest_rmi_names:
      idx = [i for i, name in enumerate(forest_rmi_names) if name == f.name][0]
      forest_tmr_uuid[idx] = f.refId
      forest_tmr_catpath[idx] = f.category.name
      found.append(f.name)


not_found = [item for item in forest_tmr_names if item not in found]
if len(not_found) > 0:
  print("Forest TMR not found:")
  print(not_found)

forest_tmr_dict ={
    "names"  : forest_tmr_names,
    "uuids"  : forest_tmr_uuid,
    "units"  : forest_tmr_units,
    "values" : forest_tmr_values,
    "catpath": forest_tmr_catpath}



### Aquatic RMI

found = []
aquatic_rmi_names  = read_CF(cf_dir + "/aquatic_rmi.csv")[0]
aquatic_rmi_values = read_CF(cf_dir + "/aquatic_rmi.csv")[1]

aquatic_rmi_uuid    = [0.0] * len(aquatic_rmi_names)
aquatic_rmi_catpath = [0.0] * len(aquatic_rmi_names)
aquatic_rmi_units   = ["kg"]* len(aquatic_rmi_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in aquatic_rmi_names:
      idx = [i for i, name in enumerate(aquatic_rmi_names) if name == f.name][0]
      aquatic_rmi_uuid[idx] = f.refId
      aquatic_rmi_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in aquatic_rmi_names if item not in found]
if len(not_found) > 0:
  print("Aquatic RMI not found:")
  print(not_found)

aquatic_rmi_dict ={
    "names"  : aquatic_rmi_names,
    "uuids"  : aquatic_rmi_uuid,
    "units"  : aquatic_rmi_units,
    "values" : aquatic_rmi_values,
    "catpath": aquatic_rmi_catpath}

### Aquatic TMR

found = []

aquatic_tmr_names  = read_CF(cf_dir + "/aquatic_tmr.csv")[0]
aquatic_tmr_values = read_CF(cf_dir + "/aquatic_tmr.csv")[1]

aquatic_tmr_uuid    = [0.0] * len(aquatic_tmr_names)
aquatic_tmr_catpath = [0.0] * len(aquatic_tmr_names)
aquatic_tmr_units   = ["kg"]* len(aquatic_tmr_names)

for f in allflows_base:
  if f.category.name == "biotic":
    if f.name in aquatic_tmr_names:
      idx = [i for i, name in enumerate(aquatic_tmr_names) if name == f.name][0]
      aquatic_tmr_uuid[idx] = f.refId
      aquatic_tmr_catpath[idx] = f.category.name
      found.append(f.name)

not_found = [item for item in aquatic_tmr_names if item not in found]
if len(not_found) > 0:
  print("Aquatic TMR not found:")
  print(not_found)

aquatic_tmr_dict ={
    "names"  : aquatic_tmr_names,
    "uuids"  : aquatic_tmr_uuid,
    "units"  : aquatic_tmr_units,
    "values" : aquatic_tmr_values,
    "catpath": aquatic_tmr_catpath}



### Biotic ###
biotic_rmi_dict ={
    "names"  : agrar_rmi_names   + forest_rmi_names   + aquatic_rmi_names,
    "uuids"  : agrar_rmi_uuid    + forest_rmi_uuid    + aquatic_rmi_uuid,
    "units"  : agrar_rmi_units   + forest_rmi_units   + aquatic_rmi_units,
    "values" : agrar_rmi_values  + forest_rmi_values  + aquatic_rmi_values,
    "catpath": agrar_rmi_catpath + forest_rmi_catpath + aquatic_rmi_catpath}

biotic_tmr_dict ={
    "names"  : agrar_tmr_names   + forest_tmr_names   + aquatic_tmr_names,
    "uuids"  : agrar_tmr_uuid    + forest_tmr_uuid    + aquatic_tmr_uuid,
    "units"  : agrar_tmr_units   + forest_tmr_units   + aquatic_tmr_units,
    "values" : agrar_tmr_values  + forest_tmr_values  + aquatic_tmr_values,
    "catpath": agrar_tmr_catpath + forest_tmr_catpath + aquatic_tmr_catpath}


### Totals ###

total_rmi_dict ={
    "names"  : abiotic_rmi_dict["names"]    + biotic_rmi_dict["names"],
    "uuids"  : abiotic_rmi_dict["uuids"]    + biotic_rmi_dict["uuids"],
    "units"  : abiotic_rmi_dict["units"]    + biotic_rmi_dict["units"],
    "values" : abiotic_rmi_dict["values"]   + biotic_rmi_dict["values"],
    "catpath": abiotic_rmi_dict["catpath"]  + biotic_rmi_dict["catpath"]}

total_tmr_dict ={
    "names"  : abiotic_tmr_dict["names"]    + biotic_tmr_dict["names"],
    "uuids"  : abiotic_tmr_dict["uuids"]    + biotic_tmr_dict["uuids"],
    "units"  : abiotic_tmr_dict["units"]    + biotic_tmr_dict["units"],
    "values" : abiotic_tmr_dict["values"]   + biotic_tmr_dict["values"],
    "catpath": abiotic_tmr_dict["catpath"]  + biotic_tmr_dict["catpath"]}

############################################
### A D D   A G R A R   F L O W S        ###
############################################

# Because for agricultural products the characterization factors refer to reference products,
# elementary flows for agricultural RMI and TMR need to be added. Because the amount of the reference product
# is always 1, the amount of the respective elementary flow is the value of the characterization factor.
agrar_product_names = read_CF(cf_dir + "/agrar_rmi.csv")[0]

found = []

for p in allprocesses:
  # If a process with a CF is found, the respective value is exrtacted from the list
  if p.quantitativeReference.flow.name in agrar_product_names:
    idx = [i for i, name in enumerate(agrar_product_names) if name == p.quantitativeReference.flow.name][0]
    val_rmi = read_CF(cf_dir + "/agrar_rmi.csv")[1][idx]
    val_tmr = read_CF(cf_dir + "/agrar_tmr.csv")[1][idx]
    found.append(p.quantitativeReference.flow.name)

    isin = False

    # It is checked, whether this process already contains one of the elementary flows
    for ex in p.exchanges:
      if ex.flow.name == agrar_rmi_flow.name:
        isin = True

    # If not, they are added
    if isin == False:

      ex                    = model.Exchange()
      ex.isInput            = True
      ex.flow               = agrar_rmi_flow
      ex.amount             = val_rmi
      ex.unit               = kg
      ex.flowPropertyFactor = agrar_rmi_flow.referenceFactor
      ex.internalId         = 1
      p.exchanges.add(ex)
      dao_p.update(p)

      ex                    = model.Exchange()
      ex.isInput            = True
      ex.flow               = agrar_tmr_flow
      ex.amount             = val_tmr
      ex.unit               = kg
      ex.flowPropertyFactor = agrar_rmi_flow.referenceFactor
      ex.internalId         = 2
      p.exchanges.add(ex)
      dao_p.update(p)

not_found = [item for item in agrar_product_names if item not in found]
if len(not_found) > 0:
  print("Agrar products not found:")
  print(not_found)

##################################### 3. Create LCIA Method in .json files   #####################################

######################################
### E M P T Y   M E T H O D        ###
######################################

# First, the directories for the .json files of the LCIA Method are created

path_lcia_categories = mainpath + "/PMF/PMF METHOD_"+ei_version +"/lcia_categories"
path_lcia_methods    = mainpath + "/PMF/PMF METHOD_"+ei_version +"/lcia_methods"

if not os.path.exists(path_lcia_categories):
    os.makedirs(path_lcia_categories)
if not os.path.exists(path_lcia_methods):
    os.makedirs(path_lcia_methods)

# Then, uuids for the method and all the indicators are set
method_uuid = "d45eaaa9-38a6-470e-a5f7-5d41e0d5b020"

abiotic_rmi_uuid = "f6d4983c-9d4f-4015-ac90-22a517100770"
abiotic_tmr_uuid = "95a3be25-5c93-4765-826c-dc6c02bc32d6"
fossil_rmi_uuid  = "e0a124df-b1c9-499b-8ffb-74e0869083d6"
fossil_tmr_uuid  = "bf7e5a7c-0486-461f-b7e6-8f8dd39221e9"
metal_rmi_uuid   = "a8695d60-5bcb-43b2-9329-3241c341a609"
metal_tmr_uuid   = "371bb814-aa8d-4909-8fa1-68b29d4e46fa"
mineral_rmi_uuid = "f8d829ed-e925-403a-9889-b508745b37c3"
mineral_tmr_uuid = "388cad92-50d4-4dfe-ae57-1ba7e39d5bbc"

biotic_rmi_uuid  = "9f3b9cf9-6de2-42c5-8b78-62d7a43eaeb7"
biotic_tmr_uuid  = "51d474bf-0f08-469b-b928-9b9a19f84f25"
agrar_rmi_uuid   = "636fa734-c524-467a-85b7-f019ee1371b8"
agrar_tmr_uuid   = "0185ef12-3f52-4715-9f66-3d936ed9ac0e"
forest_rmi_uuid  = "92c3f79d-baaa-498c-929c-64e3cbac0cc9"
forest_tmr_uuid  = "d60d73a8-a2b9-4ae0-b294-9e4b4fbca0f7"
aquatic_rmi_uuid = "5d3af345-271e-4cde-9957-febf87eda510"
aquatic_tmr_uuid = "0900b5bf-9a96-4834-aa23-c892dd34187e"

total_rmi_uuid = "57ef9fd6-2f52-4d2e-96ad-1615fa97c835"
total_tmr_uuid = "3e3fa5e4-4a1a-4c20-a1b7-85a71ca2e749"


# Dictionaries for the .json files are created
method={"@type":"ImpactMethod","category":"PMF","@id":method_uuid,"name":"PMF","version":"1.0",
  "impactCategories":[
  {"@type":"ImpactCategory","category":"PMF","@id":abiotic_rmi_uuid,"name":"Abiotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":abiotic_tmr_uuid,"name":"Abiotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":fossil_rmi_uuid,"name":"Fossil RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":fossil_tmr_uuid,"name":"Fossil TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":metal_rmi_uuid,"name":"Metal RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":metal_tmr_uuid,"name":"Metal TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":mineral_rmi_uuid,"name":"Mineral RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":mineral_tmr_uuid,"name":"Mineral TMR","refUnit":"kg"},

  {"@type":"ImpactCategory","category":"PMF","@id":biotic_rmi_uuid,"name":"Biotic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":biotic_tmr_uuid,"name":"Biotic TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":agrar_rmi_uuid,"name":"Agrar RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":agrar_tmr_uuid,"name":"Agrar TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":forest_rmi_uuid,"name":"Forest RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":forest_tmr_uuid,"name":"Forest TMR","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":aquatic_rmi_uuid,"name":"Aquatic RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":aquatic_tmr_uuid,"name":"Aquatic TMR","refUnit":"kg"},

  {"@type":"ImpactCategory","category":"PMF","@id":total_rmi_uuid,"name":"Total RMI","refUnit":"kg"},
  {"@type":"ImpactCategory","category":"PMF","@id":total_tmr_uuid,"name":"Total TMR","refUnit":"kg"}
  ]}



empty_abiotic_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": abiotic_rmi_uuid,
    "name": "Abiotic RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": abiotic_rmi_uuid + ".json"}

empty_abiotic_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": abiotic_tmr_uuid,
    "name": "Abiotic TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": abiotic_tmr_uuid + ".json"}

empty_fossil_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": fossil_rmi_uuid,
    "name": "Fossil RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": fossil_rmi_uuid +".json"}

empty_fossil_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": fossil_tmr_uuid,
    "name": "Fossil TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": fossil_tmr_uuid + ".json"}


empty_metal_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": metal_rmi_uuid,
    "name": "Metal RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": metal_rmi_uuid + ".json"}

empty_metal_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": metal_tmr_uuid,
    "name": "Metal TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": metal_tmr_uuid + ".json"}


empty_mineral_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": mineral_rmi_uuid,
    "name": "Mineral RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": mineral_rmi_uuid + ".json"}

empty_mineral_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": mineral_tmr_uuid,
    "name": "Mineral TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": mineral_tmr_uuid + ".json"}


empty_biotic_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": biotic_rmi_uuid,
    "name": "Biotic RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": biotic_rmi_uuid +".json"}


empty_biotic_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": biotic_tmr_uuid,
    "name": "Biotic TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": biotic_tmr_uuid + ".json"}

empty_agrar_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": agrar_rmi_uuid,
    "name": "Agrar RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": agrar_rmi_uuid +".json"}


empty_agrar_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": agrar_tmr_uuid,
    "name": "Agrar TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": agrar_tmr_uuid + ".json"}

empty_forest_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": forest_rmi_uuid,
    "name": "forest RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": forest_rmi_uuid +".json"}


empty_forest_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": forest_tmr_uuid,
    "name": "forest TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": forest_tmr_uuid + ".json"}


empty_aquatic_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": aquatic_rmi_uuid,
    "name": "Aquatic RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": aquatic_rmi_uuid +".json"}


empty_aquatic_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": aquatic_tmr_uuid,
    "name": "Aquatic TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": aquatic_tmr_uuid + ".json"}

empty_total_rmi  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": total_rmi_uuid,
    "name": "Total RMI",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": total_rmi_uuid +".json"}


empty_total_tmr  ={
    "@context": "http://greendelta.github.io/olca-schema/context.jsonld",
    "@type": "ImpactCategory",
    "category":"PMF",
    "@id": total_tmr_uuid,
    "name": "Total TMR",
    "version": "1.0",
    "lastChange": "2022-02-06T17:59:25.947+02:00",
    "referenceUnitName": "kg",
    "impactFactors": [],
    "id": total_tmr_uuid + ".json"}

# The dictionaries are saved as .json-files


with open(path_lcia_methods + '/' + method_uuid + '.json', 'w') as fp:
  json.dump(method, fp)

with open(path_lcia_categories + '/' + abiotic_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_abiotic_rmi, fp)
with open(path_lcia_categories + '/'+ abiotic_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_abiotic_tmr, fp)
with open(path_lcia_categories + '/'+ fossil_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_fossil_rmi, fp)
with open(path_lcia_categories + '/'+ fossil_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_fossil_tmr, fp)
with open(path_lcia_categories + '/'+ metal_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_metal_rmi, fp)
with open(path_lcia_categories + '/'+ metal_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_metal_tmr, fp)
with open(path_lcia_categories + '/'+ mineral_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_mineral_rmi, fp)
with open(path_lcia_categories + '/'+ mineral_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_mineral_tmr, fp)

with open(path_lcia_categories + '/'+ biotic_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_biotic_rmi, fp)
with open(path_lcia_categories + '/'+ biotic_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_biotic_tmr, fp)
with open(path_lcia_categories + '/'+ agrar_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_agrar_rmi, fp)
with open(path_lcia_categories + '/'+ agrar_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_agrar_tmr, fp)
with open(path_lcia_categories + '/'+ forest_rmi_uuid + '.json', 'w') as fp:
    json.dump(empty_forest_rmi, fp)
with open(path_lcia_categories + '/'+ forest_tmr_uuid +'.json', 'w') as fp:
    json.dump(empty_forest_tmr, fp)
with open(path_lcia_categories + '/'+ aquatic_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_aquatic_rmi, fp)
with open(path_lcia_categories + '/'+ aquatic_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_aquatic_tmr, fp)

with open(path_lcia_categories + '/'+ total_rmi_uuid + '.json', 'w') as fp:
  json.dump(empty_total_rmi, fp)
with open(path_lcia_categories + '/'+ total_tmr_uuid +'.json', 'w') as fp:
  json.dump(empty_total_tmr, fp)

##################################################
### P O P U L A T E     .J S O N - F I L E S #####
##################################################


PMF_uuid = [
    abiotic_rmi_uuid + ".json",
    abiotic_tmr_uuid + ".json",
    fossil_rmi_uuid + ".json",
    fossil_tmr_uuid + ".json",
    metal_rmi_uuid + ".json",
    metal_tmr_uuid + ".json",
    mineral_rmi_uuid + ".json",
    mineral_tmr_uuid + ".json",

    biotic_rmi_uuid + ".json",
    biotic_tmr_uuid + ".json",
    agrar_rmi_uuid + ".json",
    agrar_tmr_uuid + ".json",
    forest_rmi_uuid + ".json",
    forest_tmr_uuid + ".json",
    aquatic_rmi_uuid + ".json",
    aquatic_tmr_uuid + ".json",

    total_rmi_uuid + ".json",
    total_tmr_uuid + ".json"
]


PMF_dict = {
    "Abiotic RMI": abiotic_rmi_dict,
    "Abiotic TMR": abiotic_tmr_dict,
    "Fossil RMI": fossil_rmi_dict,
    "Fossil TMR": fossil_tmr_dict,
    "Metal RMI": metal_rmi_dict,
    "Metal TMR": metal_tmr_dict,
    "Mineral RMI": mineral_rmi_dict,
    "Mineral TMR": mineral_tmr_dict,

    "Biotic RMI": biotic_rmi_dict,
    "Biotic TMR": biotic_tmr_dict,
    "Agrar RMI": agrar_rmi_dict,
    "Agrar TMR": agrar_tmr_dict,
    "Forest RMI": forest_rmi_dict,
    "Forest TMR": forest_tmr_dict,
    "Aquatic RMI": aquatic_rmi_dict,
    "Aquatic TMR": aquatic_tmr_dict,

    "Total RMI": total_rmi_dict,
    "Total TMR": total_tmr_dict
}

cat_names = [
    "Abiotic RMI",
    "Abiotic TMR",
    "Fossil RMI",
    "Fossil TMR",
    "Metal RMI",
    "Metal TMR",
    "Mineral RMI",
    "Mineral TMR",

    "Biotic RMI",
    "Biotic TMR",
    "Agrar RMI",
    "Agrar TMR",
    "Forest RMI",
    "Forest TMR",
    "Aquatic RMI",
    "Aquatic TMR",

  	"Total RMI",
    "Total TMR"
]


# The following for loop iterates through the dictionaries and
# populates the json files with the relevant information

for i in range(0,len(PMF_uuid)):
  f_in = os.path.join(path_lcia_categories,PMF_uuid[i])
  with open(f_in, 'r+') as f:
    thisd = json.load(f)
    thisd['name'] = cat_names[i]
    thisd['id']   = PMF_uuid[i]
    del thisd['impactFactors'][0:len(thisd['impactFactors'])] # delete the two factors that are still there from copying the files

    for mli in range(0,len(PMF_dict[cat_names[i]]["values"])):
      CF = CF_generate(mli,
                       Val = PMF_dict[cat_names[i]]["values"],
                       dnames = PMF_dict[cat_names[i]]["names"],
                       duuid = PMF_dict[cat_names[i]]["uuids"],
                       dcatpath = "",
                       dunit = PMF_dict[cat_names[i]]["units"])
      # add new CF to json file:
      if CF["value"] > 0:
        thisd['impactFactors'].append(CF)

    # wrap up and save
    f.seek(0)        # reset file position to the beginning.
    json.dump(thisd, f, indent=4)
    f.truncate()     # remove remaining part
    f.close()


# Save the Method
shutil.make_archive(mainpath + "/PMF/PMF METHOD_"+ ei_version, 'zip', root_dir=mainpath + "/PMF/PMF METHOD_"+ ei_version)
method_dir = mainpath + "/PMF/PMF METHOD_" + ei_version + ".zip"
print("PMF method created :)")


##################################### 4. Import PMF LCIA Method #####################################

# The LCIA-Method is imported into the current database
reader = ZipStore.open(File(method_dir))
i = JsonImport(reader,db)
i.run()
reader.close()
