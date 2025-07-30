# FLO-NAVSAFE: Flood Navigation and Safety Application

The **Flood Inundation Map (FIM)** displayed on the home page of the FLO-NAVSAFE application is sourced from NOAA’s National Water Model (NWM):  
[https://maps.water.noaa.gov/server/rest/services/nwm/ana_inundation_extent/FeatureServer/0](https://maps.water.noaa.gov/server/rest/services/nwm/ana_inundation_extent/FeatureServer/0)

---

## 🚀 Forecast Page and Automation Pipeline  

The forecast page of the application is powered by an automated workflow located in the `Scripts` folder:

- **get_nwm_sr_forecast.py** – Downloads short-range discharge forecasts from the NWM.  
- **Fim_generation.py** – Uses the ArcGIS Pro Python environment to generate forecasted Flood Inundation Maps (FIMs).  
- **automation.py** – Orchestrates the hourly execution of `get_nwm_sr_forecast.py` and `Fim_generation.py`.  

All required Python packages for running the scripts are listed in **`nwm.yml`**.

---

## 📓 Jupyter Notebooks  

Fully documented notebooks are provided in the `Notebooks` folder for transparency and reproducibility:

- **get_nwm_sr_forecast.ipynb** – Walkthrough of the discharge download process.  
- **FIM_generation.ipynb** – Step-by-step documentation of the FIM generation workflow.  

---

## 📂 Data Sources  

The following datasets are used in FLO-NAVSAFE and are available for download via HydroShare:  
👉 [**HydroShare Resource Link**](https://hydroshare.org/resource/41b23520d92c4d6e8d7934f106e54fd3/)

- **Low Water Crossing dataset**  
- **Travis County shapefile**  
- **Critical Infrastructure maps**  
- **Social Vulnerability maps**  

Other supporting datasets used within the scripts are also included in the HydroShare resource.

---
