"""
Downloading the meteo data from the STAC server (MeteoSwiss)
"""

import os
import sys
sys.path.append(os.path.abspath(r"/"))

import pandas as pd
import requests
from io import StringIO
import warnings

# Ignore FutureWarnings
warnings.filterwarnings('ignore', category=FutureWarning)

# ==========================================================
# === Global parameters and station definitions ============
# ==========================================================

# Variables to be extracted from MeteoSwiss and their renamed form
variables = {
            'prestah0': 'atmospheric_pressure',
            'tre200h0': 'air_temperature',
            'rre150h0': 'rainfall',
            'ure200h0': 'humidity',
            'dkl010h0': 'wind_direction',
            'gre000h0': 'global_radiation',
            'oli000h0': 'irradiation',
            'fkl010h0': 'wind_speed',
        }

# Dictionary mapping stations to cantons, populations, and weighting parameter
station_to_canton = {
    'Adelboden': {
        'station_id': 'ABO',
        'canton': 'BE',
        'population': 3340,
        'district': 40674,
        'canton_pop': 1047473
    }, # 3340, Verwaltungskreis Frutigen-Niedersimmental 40 674, BE 1 047 473
    'Bern_Zollikofen': {
        'station_id': 'BER',
        'canton': 'BE',
        'population': 10825,
        'district': 418858,
        'canton_pop': 1047473
    }, # 10825, Verwaltungskreis Bern-Mittelland 418858, BE 1 047 473
    'Interlaken': {
        'station_id': 'INT',
        'canton': 'BE',
        'population': 5821,
        'district': 47811,
        'canton_pop': 1047473
    }, # 5821, Verwaltungskreis Interlaken-Oberhasli 47811, BE 1 047 473
    'Basel_Binningen': {
        'station_id': 'BAS',
        'canton': 'BL',
        'population': 15616,
        'district': 157641,
        'canton_pop': 292817
    }, # 15616, Bezirk Arlesheim 157641, Basel-Landschaft 292817 ((Basel-Stadt 196036))
    'Geneve_Cointrin': {
        'station_id': 'GVE',
        'canton': 'GE',
        'canton_pop': 509448
    }, # GE 509448
    'Chur_Ems': {
        'station_id': 'CHU',
        'canton': 'GR',
        'population': 37875,
        'district': 43233,
        'canton_pop': 201376
    }, # 37875, Region Plessur 43233, GR 201376
    'Disentis': {
        'station_id': 'DIS',
        'canton': 'GR',
        'population': 2033,
        'district': 21438,
        'canton_pop': 201376
    }, # 2033, Region Surselva 21438, GR 201376
    'Davos': {
        'station_id': 'DAV',
        'canton': 'GR',
        'population': 10648,
        'district': 26060,
        'canton_pop': 201376
    }, # 10648, Region Prättigau/Davos 26060, GR 201376
    'Samedan': {
        'station_id': 'SAM',
        'canton': 'GR',
        'population': 3035,
        'district': 18236,
        'canton_pop': 201376
    }, # 2915, Region Maloja 18236, GR 201376
    'Luzern': {
        'station_id': 'LUZ',
        'canton': 'LU',
        'canton_pop': 420326
    }, # 8180, LU 420326
    'Neuchatel': {
        'station_id': 'NEU',
        'canton': 'NE',
        'population': 44485,
        'district': 176166,
        'canton_pop': 176166
    }, # 44485, NE 176166
    'Buchs_Suhr': {
        'station_id': 'BUS',
        'canton': 'AG',
        'population': 8270,
        'district': 81275,
        'canton_pop': 703086
    }, # 8270, Bezirk Aarau 81275, AG 703086
    'Engelberg': {
        'station_id': 'ENG',
        'canton': 'OW',
        'canton_pop': 38435
    }, #4230, OW 38435
    'St_Gallen': {
        'station_id': 'STG',
        'canton': 'SG',
        'population': 76328,
        'district': 123274,
        'canton_pop': 519245
    }, # 76328, Wahlkreis St. Gallen 123274, SG 519245
    'Schaffhausen': {
        'station_id': 'SHA',
        'canton': 'SH',
        'population': 37248,
        'district': 55740,
        'canton_pop': 83995
    },  # 37248, Bezirk Schaffhausen  55740, SH  83995
    'Lugano': {
        'station_id': 'LUG',
        'canton': 'TI',
        'population': 62123,
        'district': 151242,
        'canton_pop': 352181
    }, # 62123, Distretto di Lugano 151242, TI 352181
    'Piotta': {
        'station_id': 'PIO',
        'canton': 'TI',
        'population': 974,
        'district': 8718,
        'canton_pop': 352181
    }, # 974, Distretto di Leventina 8718, TI 352181
    'Altdorf': {
        'station_id': 'ALT',
        'canton': 'UR',
        'canton_pop': 37047,
    }, # 9719, UR 37047
    'Pully': {
        'station_id': 'PUY',
        'canton': 'VD',
        'population': 18128,
        'district': 64270,
        'canton_pop': 822968
    }, # 18928, Disctrict de Lavaux-Oron  64270, VD 822968
    'Sion': {
        'station_id': 'SIO',
        'canton': 'VS',
        'population': 35259,
        'district': 49023,
        'canton_pop': 353209
    }, # 35259, District de Sion  49023, VS 353209
    'Zermatt': {
        'station_id': 'ZER',
        'canton': 'VS',
        'population': 5769,
        'district': 28706,
        'canton_pop': 353209
    }, # 5769, District de Viège  28706, VS 353209
    'Zuerich_Kloten': {
        'station_id': 'KLO',
        'canton': 'ZH',
        'canton_pop': 1564662
    }, # ZH 1564662
}

default_config = {
    'weight_weekday_type': True,  # penalization if day type mismatch (weekday/weekend)
    'params_weights': {
        'fkl010h1': 0,      # secondary wind speed
        'prestahs': 0,      # static pressure
        'tre200h0': 1,      # temperature
        'rre150h0': 0,      # precipitation
        'ure200h0': 0.5,    # relative humidity
        'dkl010h0': 0,      # wind direction
        'gre000h0': 1,      # global radiation
        'oli000h0': 0,      # irradiation
        'fkl010h0': 0.5,    # wind speed
        'day_type': 900,    # heavy penalty for mismatched day type
        'water_stock': 1,   # optional variable: snow/water accumulation
        'time_window': 800, # penalty if day is outside selected window
        'temp_lag': 1,      # lagged temperature variable
    },
    'time_window': -1,       # penalization if day outside window
    'add_water_stock': True, # include water stock variable
    'percentile': 0.05,      # percentile used for nearest-day selection
    'n_nearests': 5,         # number of nearest days (for grid search)
    'temp_lag': 3,           # lag (days) for temperature variable
}

# ==========================================================
# === Core functions =======================================
# ==========================================================

def load_meteo_file(config, is_verbose=False):
    """
    Load and process meteorological data for all configured stations.

    This function loops over all defined weather stations, downloads the
    corresponding MeteoSwiss data via the STAC API, applies population-based
    weighting (if enabled), concatenates results horizontally, and returns
    a cleaned, merged dataset.

    Parameters
    ----------
    config : object
        Configuration object containing:
        - `freq` : str → frequency code ("h", "d", "m", "y")
        - `start_date` : str (YYYY-MM-DD)
        - `end_date` : str (YYYY-MM-DD)
        - `global_meteo` : bool → if True, compute population-weighted global data
    is_verbose : bool, optional
        If True, print detailed information during download and processing.

    Returns
    -------
    pd.DataFrame
        Concatenated and cleaned meteorological dataset for all stations.
    """
    meteo = {}
    meteo_global = {}
    station_to_canton = compute_station_weights()
    for name, station in station_to_canton.items():
        station_id = station['station_id']  # station id
        freq = config.freq
        start_date = config.start_date
        end_date = config.end_date
        # downloading the meteo data for each station
        meteo[station_id] = download_meteo_file(station_id, start_date, end_date, freq, is_verbose=is_verbose)
        # create the global meteo data columns
        if config.global_meteo:
            meteo_global[station_id] = meteo[station_id]*station['weight']
    meteo['global'] = pd.concat(meteo_global.values(), axis=1).groupby(level=0, axis=1).sum()
    data = concat_stations_horizontal(meteo, exclude_keys=None)
    # Save a csv of the stations data and their wieght
    station_csv = pd.DataFrame.from_dict(station_to_canton, orient='index').reset_index()
    station_csv.rename(columns={'index': 'Name'}, inplace=True)
    station_csv.to_csv('./data/station_weight.csv', index=False, encoding='utf-8-sig')
    # Remove columns that contain only NaN values
    data = data.dropna(axis=1, how='all')
    return data


def compute_station_weights():
    """
    Compute population-based weights for each Swiss meteorological station.

    This function calculates the relative weight of each station
    considering both its local population share within its canton and
    the canton’s share of the total Swiss population.

    The resulting weight reflects the contribution of each station
    to the overall meteorological mix when aggregating or averaging
    results at the national scale.

    The `station_to_canton` dictionary is **modified in place** to
    include a new key `'weight'` for each station.

    Returns
    -------
    dict
        Updated version of the global `station_to_canton` dictionary
        including computed weight factors.

    Notes
    -----
    - Each canton’s global weight = canton population / total Swiss population.
    - Each station’s local weight = (station population / total canton population).
    - Final station weight = global canton weight × local station weight.
    - Stations without population data are assigned equal weight within their canton.

    Examples
    --------
    >>> from modules.loading import compute_station_weights
    >>> station_weights = compute_station_weights()
    >>> round(station_weights['Sion']['weight'], 5)
    0.00342
    """
    cantons = {}
    tt_ch_pop = 0

    # Compute total population per canton and total Switzerland population
    for station, info in station_to_canton.items():
        if info['canton'] not in cantons:
            cantons[info['canton']] = {}
            cantons[info['canton']]['weight'] = info['canton_pop']
            tt_ch_pop += info['canton_pop']

    # Normalize canton weights by total Swiss population
    for canton, pop in cantons.items():
        cantons[canton]['weight'] = pop['weight'] / tt_ch_pop

    # Compute final station weights within each canton
    for station, info in station_to_canton.items():
        canton_tt_pop = 0
        for station2, info2 in station_to_canton.items():
            if info['canton'] == info2['canton'] and 'population' in info2:
                canton_tt_pop += info2['population']
        if canton_tt_pop == 0:
            weight = 1
        else:
            weight = info['population'] / canton_tt_pop

        station_to_canton[station]['weight'] = weight * cantons[info['canton']]['weight']

    return station_to_canton



def download_meteo_file(station_id, start_date, end_date, freq, is_verbose=False):
    """
    Download meteorological data for a given station and time period.

    This function queries the MeteoSwiss STAC API to identify relevant CSV
    files for the specified frequency (e.g., hourly or daily). It automatically
    selects the `historical_*` and `recent` files that overlap with the desired
    date range, downloads and concatenates them.

    Parameters
    ----------
    station_id : str
        MeteoSwiss station code (e.g., "ABO" for Adelboden)
    start_date : str
        Start date in the format 'YYYY-MM-DD'
    end_date : str
        End date in the format 'YYYY-MM-DD'
    freq : str
        Frequency code ('h', 'd', 'm', or 'y')
    is_verbose : bool, optional
        If True, print diagnostic messages during execution.

    Returns
    -------
    pd.DataFrame
        Concatenated DataFrame of weather variables indexed by timestamp.
    """
    if is_verbose:
        print(f"Processing for station {station_id}")

    base_url = "https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items"
    params = {"q": station_id.lower(), "limit": 10}
    r = requests.get(base_url, params=params)
    r.raise_for_status()

    items = r.json().get("features", [])
    if not items:
        if is_verbose:
            print(f"❌ None item has been found for {station_id}")
        return pd.DataFrame()

    # First item = station_id
    assets = items[0]["assets"]

    # Select the relevant files based on the frequency
    relevant_files = {
        k: v["href"] for k, v in assets.items()
        if f"_{freq}_" in k and k.endswith(".csv")
    }

    if is_verbose:
        print(f"📂 {len(relevant_files)} fichiers trouvés pour {station_id}-{freq}:")
    for k in relevant_files:
        if is_verbose:
            print("   ", k)

    # Temporal filter
    def file_in_range(k):
        if "historical_" in k:
            period = k.split("_")[-1].replace(".csv", "")
            start, end = period.split("-")
            return int(start) <= int(end_date[:4]) and int(end) >= int(start_date[:4])
        return False

    selected_files = {k: v for k, v in relevant_files.items() if file_in_range(k)}

    if is_verbose:
        print(f"📅 {len(selected_files)} selected files ({start_date} → {end_date})")

    full_df = concat_meteo_items(selected_files, variables=variables, is_verbose=is_verbose)

    # Filter the dataframe to only include dates within the specified range
    if not full_df.empty and start_date and end_date:
        full_df = full_df.loc[start_date:end_date]

    return full_df



def concat_meteo_items(files_dict, variables=None, is_verbose=False):
    """
    Download and concatenate multiple MeteoSwiss CSV files.

    Iterates over a dictionary of URLs, downloads each CSV file,
    filters columns based on the given variables mapping, renames
    them, converts timestamps, and merges everything into a single
    time-indexed DataFrame.

    Parameters
    ----------
    files_dict : dict
        Dictionary of {filename: URL} pairs from the STAC API.
    variables : dict, optional
        Mapping of MeteoSwiss column codes to user-friendly names.
    is_verbose : bool, optional
        If True, print progress and download messages.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with datetime index and selected variables.
    """
    all_data = []

    for name, url in files_dict.items():
        if is_verbose:
            print(f"⬇️ Downloading {name} ...")
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text), sep=";", encoding="latin1")

            # Optional variable filter
            if variables:
                keep_cols = ["reference_timestamp"] + [v for v in variables if v in df.columns]
                df = df[keep_cols]
                # Rename columns according to the variables dictionary
                df = df.rename(columns=variables)

            all_data.append(df)
        except Exception as e:
            if is_verbose:
                print(f"⚠️ Error on {name}: {e}")

    if not all_data:
        if is_verbose:
            print("❌ No file could be downloaded.")
        return pd.DataFrame()

    # Concatenate and clean
    full_df = pd.concat(all_data, ignore_index=True)
    if "reference_timestamp" in full_df.columns:
        try:
            full_df["reference_timestamp"] = pd.to_datetime(
            full_df["reference_timestamp"],
            format="%Y-%m-%dT%H:%M:%S")
        except Exception:
            full_df["reference_timestamp"] = pd.to_datetime(
            full_df["reference_timestamp"],
            format="%d.%m.%Y %H:%M")
        full_df["reference_timestamp"] = pd.to_datetime(full_df["reference_timestamp"], errors="coerce")
        full_df = full_df.dropna(subset=["reference_timestamp"])
        #full_df = full_df.sort_values("reference_timestamp").drop_duplicates(subset="reference_timestamp")
        full_df = full_df.set_index("reference_timestamp")

    if is_verbose:
        print(f"✅ Concatenation success: {len(full_df)} rows")

    return full_df



def concat_stations_horizontal(meteo_dict, exclude_keys=None):
    """
    Concatenate meteorological station DataFrames side-by-side.

    Each station DataFrame is suffixed with its station_id to
    distinguish variable names, then merged horizontally.

    Parameters
    ----------
    meteo_dict : dict
        Dictionary mapping `station_id` to its corresponding DataFrame.
    exclude_keys : list, optional
        List of station_ids to exclude from concatenation.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with all stations’ data as separate columns.
    """
    if exclude_keys is None:
        exclude_keys = []

    dfs_to_concat = []

    for station_id, df in meteo_dict.items():
        if station_id in exclude_keys or df.empty:
            continue

        # Create a copy and add suffix to column names
        df_copy = df.copy()
        df_copy.columns = [f"{col}_{station_id}" for col in df_copy.columns]
        dfs_to_concat.append(df_copy)

    if not dfs_to_concat:
        return pd.DataFrame()

    # Concatenate horizontally
    data = pd.concat(dfs_to_concat, axis=1)

    return data