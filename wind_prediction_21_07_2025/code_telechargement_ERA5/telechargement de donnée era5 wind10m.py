import cdsapi
import xarray as xr
import pandas as pd
import numpy as np

## Telechargement de donnée 
# ###personnal data acce compte
client= cdsapi.Client(
    url='https://cds.climate.copernicus.eu/api',
    key='USER_KEY'
)

#source des type de données
dataset = "reanalysis-era5-single-levels"

#pour lancer le telechargement de plusieurs sites 
# Define the list of stations with (ID, Name, Lat, Lon) 
stations = [
    #("60394099999", "ALGER PORT, AG", 36.766667, 3.100000),
    #("60393099999", "DAR_EL_BEIDA", 36.700000, 3.200000),
    #("60381099999", "EL_HARRACH", 36.716667, 3.150000),
    #("60371099999", "OUED_KORICHE", 36.783333, 3.033333),
    #("60379099999", "REGHAIA", 36.766667, 3.333333),
    #("60380099999", "TESSALA", 36.616667, 2.916667),
    #("60374099999", "AIN_BENAIN", 36.783333, 2.883333),
    #("60376099999", "BORDJ_EL_BAHRI", 36.783333, 3.250000),
    #("60372099999", "BOUZAREAH", 36.783333, 3.000000),
    ("60373099999", "MEHELMA", 36.700000, 2.883333),
]
# determner les coordonée geographique min min max max
# Delta for bounding box
delta = 0.25

for station_id, name, lat, lon in stations:        
    df_all_years = []

    for year in range(2013, 2017): # modifeir la periode 
        filename = f'era5_wind_{station_id}_{name}_{year}.nc'
        area = [
            round(lat + delta, 2),   # North
            round(lon - delta, 2),   # West
            round(lat - delta, 2),   # South
            round(lon + delta, 2)    # East
        ]

        print(f"Downloading {name} for {year}...")

        ## type de donnée a telecharger u10 et v10 a modifier 
        
        client.retrieve(
            dataset,
            {
                "product_type": "reanalysis",
                "variable": [
                    "2m_temperature",
                     "surface_pressure",
        ],
                "year": [year],
                "month": [f"{m:02d}" for m in range(1, 13)],
                "day": [f'{d:02d}' for d in range(1, 32)],
                "time": [f"{h:02d}:00" for h in range(0, 24)],
                "format": "netcdf",
                "area": area
            },
            filename
                    )
        # ouvrire le fichier nc exemple2009
        fichier_nc = filename

          # Ouvrir le fichier
        ds = xr.open_dataset(fichier_nc)

# Affiche les variables disponibles
        #print(ds.data_vars)
#print(ds.data_vars["u10"].sel())

        lat_site = lat
        lon_site = lon
# Sélectionne le point le plus proche ethode nearst
        #u10_point = ds['u10'].sel(latitude=lat_site, longitude=lon_site, method='nearest')
       # v10_point = ds['v10'].sel(latitude=lat_site, longitude=lon_site, method='nearest')
        temp_point = ds['t2m'].sel(latitude=lat_site, longitude=lon_site, method='nearest')
        pressure_point = ds['sp'].sel(latitude=lat_site, longitude=lon_site, method='nearest')
# Calcul de la vitesse du vent (m/s)
        #wind_speed = np.sqrt(u10_point**2 + v10_point**2)

# Calcul de la direction du vent (en degrés météorologiques)
      #  wind_dir = (180 + np.degrees(np.arctan2(u10_point, v10_point))) % 360

# Récupère les dates
        time = ds['valid_time'].values
# recperer la temperature
        temperature = ds['t2m']
        # recperer la temperature
        pression = ds['sp']
# Convertir les timestamps en datetime et ajouter 1h
        datetime_local = pd.to_datetime(time) + pd.Timedelta(hours=1)

# Création du DataFrame avec heure locale
        df = pd.DataFrame({
        'datetime': datetime_local,
        #'wind_speed': wind_speed.values,
        #'wind_dir': wind_dir.values
        'temp': temp_point.values,
        'pression': pressure_point.values
        })
# Sauvegarde en CSV
        df_all_years.append(df)  # Ajouter à la liste

        ds.close()  # Fermer le fichier NetCDF

 
        df_station = pd.concat(df_all_years)
        #print(df_all_years)
        df_station.to_csv(f'era5_wind_{station_id}_{name}_all_years.csv', index=False)
        print(f" Fichier combiné sauvegardé : era5_wind_{station_id}_{name}_all_years.csv")


 



