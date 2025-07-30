import subprocess
import os
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import xarray as xr
import s3fs
from IPython.display import display
import fsspec
import os, re
import fiona
import time
from datetime import datetime, date, timedelta
start= datetime.now()
# duration = timedelta(days=4)
# end = start + duration

import warnings
warnings.filterwarnings("ignore")


#Import necessary files/Define necessary parameters
#Travis County COMIDS
comid_csv = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Datasets\travis_COMIDS.csv"

#Geopackage and layer with Travis County Flowlines
flowlines_gpkg = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Nana\Theme4DataRevised\Theme4Data.gdb"
flowlines_gpkg = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\Theme4DataRevised_testing\Theme4DataRevised_testing\Theme4Data.gdb"
flowline_layer = 'P2FFlowlines'

#Output folder for shapefile
out_dir = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\discharge_shapefiles_final"
os.makedirs(out_dir, exist_ok=True)
output_shp = os.path.join(out_dir, "sr_nwm_10hr_latest.shp")

#S3 bucket info
s3_bucket = 'noaa-nwm-pds'
forecast_path = 'short_range'
variable = 'streamflow'
filename = 'channel_rt'

#Parameters
valid_time = datetime.utcnow()
max_lead_hours = 10


#Helper functions
# URL builder
def construct_s3_url(cycle_dt: datetime, lead_hr: int) -> str:
    """
    Build the S3 URL for:
      nwm.t{HH}z.short_range.{filename}.f{LLL}.conus.nc
    """
    date_str = cycle_dt.strftime("%Y%m%d")      # e.g. '20250708'
    cycle_hr = f"{cycle_dt.hour:02d}"           # e.g. '16'
    lead_str = f"{lead_hr:03d}"                 # e.g. '005'
    fname    = (
        f"nwm.t{cycle_hr}z.short_range."
        f"{filename}.f{lead_str}.conus.nc"
    )
    return f"s3://{s3_bucket}/nwm.{date_str}/{forecast_path}/{fname}"

#Function to get the most likely discharge and worst case scenario discharge
def collapse_with_runs(df):
    df = df.copy()
    # rename to the function’s expected names
    df = df.rename(
        columns={
            "forecast_r":"forecast_run",  # already correct
            "forecast_t":"forecast_time"   # already correct
        }
    )
    # parse dates
    df["forecast_r_dt"] = pd.to_datetime(df["forecast_run"])
    df["forecast_t_dt"] = pd.to_datetime(df["forecast_time"])
    
    records = []
    for fid, grp in df.groupby("feature_id", sort=False):
        valid = grp[grp["forecast_r_dt"] <= grp["forecast_t_dt"]].copy()
        valid["lead_sec"] = (valid["forecast_t_dt"] - valid["forecast_r_dt"]) \
                              .dt.total_seconds()
        top2 = valid.nlargest(2, "lead_sec")

        if not top2.empty:
            ml_dis = top2["streamflow"].max()
            ml_row = top2.loc[top2["streamflow"].idxmax()]
            ml_r   = ml_row["forecast_run"]
            ml_t   = ml_row["forecast_time"]
        else:
            ml_dis, ml_r, ml_t = None, None, None

        wc_dis = grp["streamflow"].max()
        wc_row = grp.loc[grp["streamflow"].idxmax()]
        wc_r   = wc_row["forecast_run"]
        wc_t   = wc_row["forecast_time"]

        records.append({
            "feature_id": fid,
            "ml_dis":     ml_dis,
            "ml_run":       ml_r,
            "ml_time":       ml_t,
            "wc_dis":     wc_dis,
            "wc_run":       wc_r,
            "wc_time":       wc_t
        })

    return pd.DataFrame(records)

# Read list of COMIDs
comids = pd.read_csv(comid_csv, dtype={"ID": str})["ID"].tolist()

# Read flowlines geopackage and filter
flows = gpd.read_file(flowlines_gpkg, layer=flowline_layer)
flows["feature_id"] = flows["feature_id"].astype(str)

# initialize filesystem
fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name":"us-east-1"})
today = datetime.utcnow().date()
#find most recent folder (up to 3 days back) with channel_rt files
valid_prefix = None
published_runs = set()
for delta in range(4):
    dt     = today - timedelta(days=delta)
    prefix = f"{s3_bucket}/nwm.{dt:%Y%m%d}/short_range/"
    try:
        all_files = fs.find(prefix)
    except Exception:
        continue

    runs = {
        int(m.group(1))
        for f in all_files
        if f.endswith(".conus.nc")
        and ".channel_rt." in f
        and (m := re.search(r"nwm\.t(\d{2})z", os.path.basename(f)))
    }
    if runs:
        valid_prefix   = prefix
        published_runs = runs
        break

if not valid_prefix:
    raise RuntimeError("No short_range/channel_rt folder in last 4 days")

# clear any cached listings so we see newly uploaded runs
fs.invalidate_cache(valid_prefix)

# rebuild channel_files with bare keys
channel_files = [
    f for f in fs.find(valid_prefix)
    if f.endswith(".conus.nc") and ".channel_rt." in f
]


# build a lookup of (run_hr, lead_hr) → bare S3 key
run_lead_map = {}
for path in channel_files:
    fn     = os.path.basename(path)
    m_run  = re.search(r"nwm\.t(\d{2})z", fn)
    m_lead = re.search(r"\.f(\d{3})\.", fn)
    if m_run and m_lead:
        run_hr, lead_hr = int(m_run.group(1)), int(m_lead.group(1))
        if 1 <= lead_hr <= max_lead_hours:
            run_lead_map[(run_hr, lead_hr)] = path  # <— no "s3://"

# parse the prefix date for full datetimes
date_str  = valid_prefix.split("/")[1].split(".")[1]  # e.g. "20250708"
prefix_dt = datetime.strptime(date_str, "%Y%m%d")

# Latest‐cycle forecasts (tXXZ → f001–f010)
latest_run_hr = max(run for run, _ in run_lead_map)
latest_run_dt = prefix_dt + timedelta(hours=latest_run_hr)

comids_int       = [int(c) for c in comids]
records_current  = []
chosen_run_hr    = None

for run_hr in sorted(published_runs, reverse=True):
    run_dt = prefix_dt + timedelta(hours=run_hr)
    temp   = []

    for lead_hr in range(1, max_lead_hours+1):
        key = run_lead_map.get((run_hr, lead_hr))
        if not key:
            continue

        try:
            with fs.open(key, "rb") as fobj:
                ds = xr.open_dataset(fobj, engine="h5netcdf")

                # integer‐based intersection
                ds_ids      = ds["feature_id"].values.astype(int)
                present_int = [cid for cid in comids_int if cid in ds_ids]

                if not present_int:
                    continue

                da = ds[variable].sel(feature_id=present_int).load()
        except (FileNotFoundError, OSError):
            continue

        # build the DataFrame for this slice
        df = da.to_dataframe().reset_index()
        df["feature_id"]   = df["feature_id"].astype(str)
        df = df.set_index("feature_id").reindex(comids).reset_index()
        df["valid_time"]   = run_dt + timedelta(hours=lead_hr)
        df["forecast_run"] = run_dt
        df["lead_hour"]    = lead_hr
        temp.append(df)

    if temp:
        chosen_run_hr   = run_hr
        records_current = temp
        break

if not records_current:
    raise RuntimeError("No 1–10h forecasts found for any recent run")

latest_run_hr = chosen_run_hr
latest_run_dt = prefix_dt + timedelta(hours=latest_run_hr)
df_current    = pd.concat(records_current, ignore_index=True)


# pull ensemble for each valid_time 
records_ensemble = []
valid_times      = [latest_run_dt + timedelta(hours=h) for h in range(1, max_lead_hours+1)]

for valid_time in valid_times:
    for lead_hr in range(1, max_lead_hours+1):
        run_dt = valid_time - timedelta(hours=lead_hr)
        key    = run_lead_map.get((run_dt.hour, lead_hr))
        if not key:
            continue

        try:
            with fs.open(key, "rb") as fobj:
                ds = xr.open_dataset(fobj, engine="h5netcdf")

                ds_ids      = ds["feature_id"].values.astype(int)
                present_int = [cid for cid in comids_int if cid in ds_ids]
                if not present_int:
                    continue

                da = ds[variable].sel(feature_id=present_int).load()
        except (FileNotFoundError, OSError):
            continue

        df = da.to_dataframe().reset_index()
        df["feature_id"]   = df["feature_id"].astype(str)
        df = df.set_index("feature_id").reindex(comids).reset_index()
        df["valid_time"]   = valid_time
        df["forecast_run"] = run_dt
        df["lead_hour"]    = lead_hr
        records_ensemble.append(df)

df_ensemble = pd.concat(records_ensemble, ignore_index=True)


# Re-concat the full ensemble
df_all = pd.concat(records_ensemble, ignore_index=True)
# Rename valid_time → forecast_time
df_all = df_all.rename(columns={"valid_time": "forecast_time"})

# Make sure your datetime columns are Timestamps
df_all['forecast_run'] = pd.to_datetime(df_all['forecast_run'])
df_all['forecast_time']   = pd.to_datetime(df_all['forecast_time'])

# compute “next hour” off your chosen latest run
latest_run = df_current["forecast_run"].max()
next_hour = latest_run + timedelta(hours=1)

# grab **all** forecasts (from any run) that land on that time  
df1_multi = df_all.query(
    "forecast_time == @next_hour and lead_hour >= 1"
)[["feature_id","forecast_time","forecast_run", variable]]

# rename and merge as before
df1_multi = df1_multi.rename(columns={variable:"streamflow"})
df1_multi["streamflow"] = df1_multi["streamflow"].fillna(0)
# Calculate wc_dis and ml_dis 
collapse_1h = collapse_with_runs(df1_multi)

gdf1_multi = flows[["LakeID",
    "HydroID",
    "From_Node",
    "To_Node",
    "NextDownID",
    "feature_id",
    "order_",
    "areasqkm",
    "geometry"]].merge(
    collapse_1h, on="feature_id", how="left"
)

for col in ["ml_time","wc_time","ml_run","wc_run"]:
    # if they’re pandas Timestamps, use strftime for consistent formatting
    if pd.api.types.is_datetime64_any_dtype(gdf1_multi[col]):
        gdf1_multi[col] = gdf1_multi[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        gdf1_multi[col] = gdf1_multi[col].astype(str)
path1 = os.path.join(out_dir, "sr_nwm_tc_1hour.shp")
#print(gdf1_multi)
gdf1_multi.to_file(path1)


#For 2 hour shapefile
# compute the target datetime for “2 hours ahead of the latest run”
latest_run = df_current["forecast_run"].max()
target_time = latest_run + timedelta(hours=2)

# pull every forecast (any run) that lands on that exact datetime
df2_multi = df_all.query("forecast_time == @target_time")[
    ["feature_id","forecast_time","forecast_run", variable]
].rename(columns={variable: "streamflow"})
df2_multi["streamflow"] = df2_multi["streamflow"].fillna(0)
# Calculate wc_dis and ml_dis 
collapse_2h = collapse_with_runs(df2_multi)

# merge on your flowlines (keeping exactly the columns you want)
keep = [
    "LakeID", "HydroID", "From_Node", "To_Node",
    "NextDownID", "feature_id", "order_", "areasqkm", "geometry"
]
gdf2_multi = (
    flows[keep]
      .merge(collapse_2h, on="feature_id", how="left")
)

for col in ["ml_time","wc_time","ml_run","wc_run"]:
    # if they’re pandas Timestamps, use strftime for consistent formatting
    if pd.api.types.is_datetime64_any_dtype(gdf2_multi[col]):
        gdf2_multi[col] = gdf2_multi[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        gdf2_multi[col] = gdf2_multi[col].astype(str)
# write out to shapefile
path2 = os.path.join(out_dir, "sr_nwm_tc_2hour.shp")
gdf2_multi.to_file(path2)

#Max 10 hour discharge
#get latest initialization time
latest_run = df_current["forecast_run"].max()

# compute the per‐reach max across the full 1–10 h ensemble
df10 = (
    df_all
      # df_all has: feature_id, forecast_run, forecast_time, lead_hour, variable
      .groupby("feature_id")[variable]
      .max()
      .reset_index()
      .rename(columns={variable: "streamflow"})
)

# add the datetime (latest run) as a string
df10["datetime"] = latest_run.strftime("%Y-%m-%d %H:%M:%S")

#merge onto your flowlines, keeping exactly the attributes you want:
keep = [
    "LakeID", "HydroID", "From_Node", "To_Node",
    "NextDownID", "feature_id", "order_", "areasqkm", "geometry"
]
gdf10 = (
    flows[keep]
      .merge(df10, on="feature_id", how="left")
)

#fill any missing reaches with zero
gdf10["streamflow"] = gdf10["streamflow"].fillna(0)

# write out your shapefile
path10 = os.path.join(out_dir, "sr_nwm_tc_max10hr.shp")
gdf10.to_file(path10)
print(f"Completed")
