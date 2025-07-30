import arcpy
import pandas as pd
import os
import uuid
import traceback
import datetime as dt
from collections import defaultdict
import datetime as dt
from dateutil.parser import parse as parse_dt
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import time
from datetime import datetime, date, timedelta
# start= datetime.now()
# duration = timedelta(days=4)
# end = start + duration
import warnings
warnings.filterwarnings("ignore")


# === Set your environment ===
arcpy.env.workspace = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Theme4DataRevised_testing\Theme4DataRevised_testing"
arcpy.env.overwriteOutput = True

# Auxiliary data needed
#rating_curve_table = r"C:\Mac\Home\Documents\ArcGIS\Projects\Theme4DataRevised\Theme4Data.gdb\RatingCurves"
rating_curve_table = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Theme4DataRevised_testing\Theme4DataRevised_testing\Theme4Data.gdb\RatingCurves"
rating_curve= rating_curve_table 
#flood_stack = r"C:\Mac\Home\Documents\ArcGIS\Projects\Theme4DataRevised\Theme4Data.gdb\TravisFIM"
flood_stack = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Theme4DataRevised_testing\Theme4DataRevised_testing\Theme4Data.gdb\TravisFIM"
output_gdb = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Theme4DataRevised_testing\Theme4DataRevised_testing\Theme4Data.gdb"

# Shapefile with flows
# shp_file = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\shape_nana\sr_nwm_tc_max10hr.shp"

### For our 3 FIMs

#folder = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\discharge_shapefiles_2"
folder = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\discharge_shapefiles_final"
def safe_add_field(table, field_name, field_type):
    """Add field only if it doesn't already exist."""
    if field_name.lower() not in [f.name.lower() for f in arcpy.ListFields(table)]:
        arcpy.AddField_management(table, field_name, field_type)

def process_fim_flow(in_fc, flow_field, out_name):
    # clear any leftover locks
    arcpy.ClearWorkspaceCache_management()

    # 1) Bring your source in as a layer (no locks on the file itself)
    fc_lyr = arcpy.MakeFeatureLayer_management(in_fc, "fc_tmp").getOutput(0)

    # 2) add & calculate Q_cfs on that layer
    safe_add_field(fc_lyr, "Q_cfs", "DOUBLE")
    arcpy.CalculateField_management(
        fc_lyr, "Q_cfs", f"!{flow_field}! * 1", "PYTHON3"
    )

    # 3) copy the layer into scratch/in_memory to work on it
    scratch = arcpy.env.scratchGDB
    working = arcpy.CopyFeatures_management(
        fc_lyr, os.path.join(scratch, f"{out_name}_wrk")
    ).getOutput(0)

    # 4) delete the temp layer immediately
    arcpy.Delete_management(fc_lyr)
    
    # 2) interpolate Q → H
    arcpy.ImportToolbox(
        r"C:\Program Files\ArcGIS\Pro\Resources\ArcToolbox\Toolboxes\Arc_Hydro_Tools_Pro.tbx",
        "hydro"
    )
    arcpy.interpolatefromlookuptablewithfieldmappings_hydro(
        working, rating_curve,
        "Q to H", "Q_cfs:discharge_","H:stage_m",
        "HydroID","HydroID"
    )
    # 3) compute HIndex only where H is non-null
    lyr = arcpy.MakeFeatureLayer_management(working, "hasH", "H IS NOT NULL").getOutput(0)
    arcpy.CalculateField_management(
        lyr, "HIndex",
        "int(round(!H! *3.28) + 1)", "PYTHON3"
    )
    arcpy.Delete_management(lyr)

    # 4) spatial join to flood polygons
    out_fc = os.path.join(output_gdb, out_name)
    if arcpy.Exists(out_fc):
        arcpy.Delete_management(out_fc)

    flood_lyr  = arcpy.MakeFeatureLayer_management(flood_stack, "flood_tmp").getOutput(0)
    stream_lyr = arcpy.MakeFeatureLayer_management(working,     "strm_tmp").getOutput(0)
    
    # cast join keys to LONG
    for fld in ("HydroID", "HIndex"):
        field_type = "LONG" if fld == "HydroID" else "DOUBLE"
        safe_add_field(stream_lyr, fld + "_num", field_type)
        arcpy.CalculateField_management(
            stream_lyr, fld + "_num", f"!{fld}!", "PYTHON3"
    )


    arcpy.analysis.SpatialJoin(
        flood_lyr, stream_lyr, out_fc,
        "JOIN_ONE_TO_MANY", "KEEP_COMMON",
        match_fields=[["HydroID_num","HydroID"], ["HIndex_num","HIndex"]]
    )
    
    # cleanup
    for tmp in (flood_lyr, stream_lyr, working):
        arcpy.Delete_management(tmp)

    arcpy.Delete_management(working)

    print(f" {out_name} written to {output_gdb}")


def process_fim_flow(in_fc, flow_field, out_name):
    # clear any leftover locks
    arcpy.ClearWorkspaceCache_management()

    # 1) Bring your source in as a layer (no locks on the file itself)
    fc_lyr = arcpy.MakeFeatureLayer_management(in_fc, "fc_tmp").getOutput(0)

    # 2) add & calculate Q_cfs on that layer
    safe_add_field(fc_lyr, "Q_cfs", "DOUBLE")
    arcpy.CalculateField_management(
        fc_lyr, "Q_cfs", f"!{flow_field}! * 1", "PYTHON3"
    )

    # 3) copy the layer into scratch/in_memory to work on it
    scratch = arcpy.env.scratchGDB
    working = arcpy.CopyFeatures_management(
        fc_lyr, os.path.join(scratch, f"{out_name}_wrk")
    ).getOutput(0)

    # 4) delete the temp layer immediately
    arcpy.Delete_management(fc_lyr)
    
    # Interpolate Q → H
    arcpy.ImportToolbox(
        r"C:\Program Files\ArcGIS\Pro\Resources\ArcToolbox\Toolboxes\Arc_Hydro_Tools_Pro.tbx",
        "hydro"
    )
    arcpy.interpolatefromlookuptablewithfieldmappings_hydro(
        working, rating_curve,
        "Q to H", "Q_cfs:discharge_","H:stage_m",
        "HydroID","HydroID"
    )

    # Compute HIndex where H is not null
    lyr = arcpy.MakeFeatureLayer_management(working, "hasH", "H IS NOT NULL").getOutput(0)
    arcpy.CalculateField_management(
        lyr, "HIndex",
        "int(round(!H! *3.28) + 1)", "PYTHON3"
    )
    arcpy.Delete_management(lyr)
    del lyr

    # Spatial join to flood polygons
    out_fc = os.path.join(output_gdb, out_name)
    if arcpy.Exists(out_fc):
        try:
            arcpy.ClearWorkspaceCache_management()
            arcpy.Delete_management(out_fc)
            print(f" Deleted existing: {out_fc}")
        except Exception as e:
            print(f" Retry deleting {out_fc} due to lock: {e}")
            time.sleep(2)
            arcpy.ClearWorkspaceCache_management()
            arcpy.Delete_management(out_fc)

   

    flood_lyr  = arcpy.MakeFeatureLayer_management(flood_stack, "flood_tmp").getOutput(0)
    stream_lyr = arcpy.MakeFeatureLayer_management(working,     "strm_tmp").getOutput(0)
    
    # Cast join keys to LONG/DOUBLE
    for fld in ("HydroID", "HIndex"):
        field_type = "LONG" if fld == "HydroID" else "DOUBLE"
        safe_add_field(stream_lyr, fld + "_num", field_type)
        arcpy.CalculateField_management(
            stream_lyr, fld + "_num", f"!{fld}!", "PYTHON3"
        )

    arcpy.analysis.SpatialJoin(
        flood_lyr, stream_lyr, out_fc,
        "JOIN_ONE_TO_MANY", "KEEP_COMMON",
        match_fields=[["HydroID_num","HydroID"], ["HIndex_num","HIndex"]]
    )
    
    # cleanup layers
    for tmp in (flood_lyr, stream_lyr):
        arcpy.Delete_management(tmp)
    del flood_lyr, stream_lyr  # ensure locks are released

    del working  # release _wrk lock
    arcpy.ClearWorkspaceCache_management()

    print(f" {out_name} written to {output_gdb}")

# ─────────────────────────────────────────────────────────────────────────────
#  Main driver: loop the folder
# ─────────────────────────────────────────────────────────────────────────────
arcpy.env.overwriteOutput = True
arcpy.env.workspace     = folder



lwc =r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Datasets\lowwatercrossing_clean\clean_LWC.shp"

def classify_lwc(fim_fc, output_name):
    # Path to original LWC
    lwc_input = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Datasets\lowwatercrossing_clean\clean_LWC.shp"
    
    # Output path for classified LWC
    lwc_out = os.path.join(output_gdb, f"{output_name}_LWC")

    # Copy the LWC shapefile into the output geodatabase
    arcpy.FeatureClassToFeatureClass_conversion(lwc_input, output_gdb, f"{output_name}_LWC")

    # Remove "Condition" if it already exists
    fields = [f.name for f in arcpy.ListFields(lwc_out)]
    if "Condition" in fields:
        arcpy.DeleteField_management(lwc_out, "Condition")

    # Add new Condition field
    arcpy.AddField_management(lwc_out, "Condition", "TEXT", field_length=10)

    # Make layer from LWC and FIM
    fim_lyr = arcpy.MakeFeatureLayer_management(fim_fc, "fim_lyr").getOutput(0)
    lwc_lyr = arcpy.MakeFeatureLayer_management(lwc_out, "lwc_lyr").getOutput(0)

    # Select only LWC points that intersect the FIM
    arcpy.SelectLayerByLocation_management(
        lwc_lyr, "INTERSECT", fim_lyr, selection_type="NEW_SELECTION"
    )

    # Set selected LWC points to "Flooded"
    arcpy.CalculateField_management(lwc_lyr, "Condition", '"Flooded"', "PYTHON3")

    # Switch selection and set others to "Safe"
    arcpy.SelectLayerByAttribute_management(lwc_lyr, "SWITCH_SELECTION")
    arcpy.CalculateField_management(lwc_lyr, "Condition", '"Safe"', "PYTHON3")

    # Cleanup
    arcpy.Delete_management(fim_lyr)
    arcpy.Delete_management(lwc_lyr)

    print(f"  {output_name}_LWC written to {output_gdb}")


def classify_address_points(fim_fc, output_name):
    # Path to original Address Points shapefile
    address_input = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Datasets\addresspoint\addresspoint.shp"
    
    # Output path for classified Address Points
    address_out = os.path.join(output_gdb, f"{output_name}_ADDR")

    # Copy to output geodatabase
    arcpy.FeatureClassToFeatureClass_conversion(address_input, output_gdb, f"{output_name}_ADDR")

    # Remove "Condition" if it exists
    fields = [f.name for f in arcpy.ListFields(address_out)]
    if "Condition" in fields:
        arcpy.DeleteField_management(address_out, "Condition")

    # Add new "Condition" field
    arcpy.AddField_management(address_out, "Condition", "TEXT", field_length=10)

    # Make layers
    fim_lyr = arcpy.MakeFeatureLayer_management(fim_fc, "fim_lyr_addr").getOutput(0)
    addr_lyr = arcpy.MakeFeatureLayer_management(address_out, "addr_lyr").getOutput(0)

    # Select points that intersect FIM
    arcpy.SelectLayerByLocation_management(
        addr_lyr, "INTERSECT", fim_lyr, selection_type="NEW_SELECTION"
    )
    arcpy.CalculateField_management(addr_lyr, "Condition", '"Flooded"', "PYTHON3")

    # Select the safe points
    arcpy.SelectLayerByAttribute_management(addr_lyr, "SWITCH_SELECTION")
    arcpy.CalculateField_management(addr_lyr, "Condition", '"Safe"', "PYTHON3")

    # Cleanup
    arcpy.Delete_management(fim_lyr)
    arcpy.Delete_management(addr_lyr)

    print(f" {output_name}_ADDR written to {output_gdb}")



generated_layers = []


for shp in arcpy.ListFiles("*.shp"):
    full = os.path.join(folder, shp)

    # 1-hour forecasts → two passes: ml_dis then wc_dis
    if "sr_nwm_tc_1hour" in shp.lower():
        out_name = "FIM_1hour_ml"
        process_fim_flow(full, "ml_dis", out_name)
        generated_layers.append(out_name)
        #classify_lwc(os.path.join(output_gdb, out_name), out_name)
        #classify_address_points(os.path.join(output_gdb, out_name), out_name)
        out_name = "FIM_1hour_wc"
        process_fim_flow(full, "wc_dis", out_name)
        generated_layers.append(out_name)
        classify_lwc(os.path.join(output_gdb, out_name), out_name)
        generated_layers.append(f"{out_name}_LWC")
        #classify_address_points(os.path.join(output_gdb, out_name), out_name)

   # 2-hour forecasts → two passes: ml_dis then wc_dis
    elif "sr_nwm_tc_2hour" in shp.lower():
        out_name = "FIM_2hour_ml"
        process_fim_flow(full, "ml_dis", out_name)
        generated_layers.append(out_name)
        #classify_lwc(os.path.join(output_gdb, out_name), out_name)
        #classify_address_points(os.path.join(output_gdb, out_name), out_name)
        out_name = "FIM_2hour_wc"
        process_fim_flow(full, "wc_dis", out_name)
        classify_lwc(os.path.join(output_gdb, out_name), out_name)
        generated_layers.append(f"{out_name}_LWC")
        #classify_address_points(os.path.join(output_gdb, out_name), out_name)

    # max-10-hour → single pass on streamflow
    elif "sr_nwm_tc_max10hr" in shp.lower():
        out_name = "FIM_10hour_wc"
        #process_fim_flow(full, "streamflow", "FIM_max_10hour")
        process_fim_flow(full, "streamflow", out_name)
        generated_layers.append(out_name)
        classify_lwc(os.path.join(output_gdb, out_name), out_name)
        generated_layers.append(f"{out_name}_LWC")
        #classify_address_points(os.path.join(output_gdb, out_name), out_name)

# Final cleanup of _wrk layers from scratch GDB
try:
    print("Cleaning up _wrk files from scratchGDB...")
    arcpy.env.workspace = arcpy.env.scratchGDB
    wrk_features = arcpy.ListFeatureClasses("*_wrk")
    for fc in wrk_features:
        arcpy.Delete_management(fc)
        print(f" Deleted leftover: {fc}")
    arcpy.ClearWorkspaceCache_management()
    print(" Cleanup complete.")
except Exception as e:
    print(f" Cleanup failed: {e}")



import arcpy
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import os

# Path to your ArcGIS Pro project and target map
aprx_path = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Flo Navsafe\Flo Navsafe.aprx"
map_name = "All_Fims2"

# === List of all generated layers (FIMs + LWC) ===
# This should be built during processing like:
# generated_layers.append(out_name)
# generated_layers.append(f"{out_name}_LWC")
# For now, assume it's already defined:
# Example:
# generated_layers = ["FIM_1hour_ml", "FIM_1hour_ml_LWC", "FIM_2hour_wc", "FIM_2hour_wc_LWC", ...]

# Open the project and target map
aprx = arcpy.mp.ArcGISProject(aprx_path)
target_map = aprx.listMaps(map_name)[0]

    
# === Step 2: Remove old FIM-related layers (those starting with 'FIM_') ===
layers_to_remove = [lyr for lyr in target_map.listLayers() if lyr.name.startswith("FIM_")]
for lyr in layers_to_remove:
    target_map.removeLayer(lyr)
print(f"Removed {len(layers_to_remove)} old FIM layers from the map.")


# Add each generated layer
for lyr_name in generated_layers:
    layer_path = os.path.join(output_gdb, lyr_name)
    if arcpy.Exists(layer_path):
        target_map.addDataFromPath(layer_path)
        print(f"Added to map: {lyr_name}")
    else:
        print(f" Skipped missing layer: {lyr_name}")

# Save the project
aprx.save()
print(" All layers added and project saved.")

# --- Step 1: Log in to ArcGIS Online ---
gis = GIS("home")

# --- Step 2: Define service info ---
service_name = "All_Fims2_WFL1"
sd_draft = r"C:\temp\All_Fims2_WFL1.sddraft"
sd_file = r"C:\temp\All_Fims2_WFL1.sd"

# --- Step 3: Create Web Layer SD Draft from the updated map ---
# map_obj = aprx.listMaps(map_name)[0]
# Make sure all references are cleared and cache is reset
arcpy.ClearWorkspaceCache_management()
del aprx  # release any hold from earlier block
aprx = arcpy.mp.ArcGISProject(aprx_path)
map_obj = aprx.listMaps(map_name)[0]


arcpy.mp.CreateWebLayerSDDraft(
    map_obj,
    sd_draft,
    service_name,
    "MY_HOSTED_SERVICES",
    "FEATURE_ACCESS",
    "",  # root folder
    True,  # overwrite_existing_service
    True,  # copy_data_to_server
    False, # editing disabled
    True,  # exporting enabled
    False, # sync disabled
    "Updated flood forecast layer",
    "flood, forecast, automation"
)

# --- Step 4: Stage the service definition ---
arcpy.StageService_server(sd_draft, sd_file)

# --- Step 5: Overwrite the hosted feature layer ---
items = gis.content.search(
    f'title:"{service_name}" AND owner:{gis.users.me.username}',
    item_type="Feature Layer",
    max_items=1
)

if not items:
    raise Exception(f" Hosted feature layer '{service_name}' not found.")

flc = FeatureLayerCollection.fromitem(items[0])
flc.manager.overwrite(sd_file)

print(f" Successfully overwritten hosted feature layer: {service_name}")

