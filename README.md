# SwissMeteo for Energy

> This module is part of the 2025 update pipeline of the **EcoDynElec** project, developed at **HEIG-VD (Energy Institute)**.

**SwissMeteo for Energy** is a Python mini-package designed to **download, aggregate and process Swiss meteorological data** from the **MeteoSwiss API**.  
It allows you to build hourly, daily, monthly or annual datasets, weighted by cantonal population, and obtain a consolidated dataset ready for energy or climate analysis.

---

## Architecture

### `modules/config.py`
This file contains the `config` class, which centralizes all user parameters needed to define:
- temporal frequency of data (`'h'`, `'d'`, `'m'`, `'y'`),
- temporal period (`start_date`, `end_date`),
- activation of global meteorological weighting (`global_meteo`),
- and save directories (`save_dir`).

#### Usage example
```python
from modules.config import config

# Create configuration
cfg = config(
    freq='d',
    start_date='2023-01-01',
    end_date='2023-12-31',
    global_meteo=True
)
```

#### Main methods

| Method | Description |
|---------|-------------|
| `set_freq(freq)` | Sets the temporal frequency. |
| `set_period(start, end)` | Modifies the temporal period. |
| `set_global_meteo(True/False)` | Activates or deactivates global weighting. |
| `get_summary()` | Returns a complete configuration summary. |

### `modules/loading.py`
This file contains all functions necessary for downloading and aggregating meteorological data from the MeteoSwiss STAC API.

#### Main functions

| Function | Description |
|----------|-------------|
| `load_meteo_file(config, is_verbose=False)` | Downloads and assembles data from all defined stations. |
| `compute_station_weights()` | Calculates the demographic weight of each station from cantonal populations. |
| `download_meteo_file(station_id, start_date, end_date, freq)` | Downloads CSV files for a given station. |
| `concat_meteo_items(files_dict, variables)` | Assembles multiple downloaded CSV files. |
| `concat_stations_horizontal(meteo_dict)` | Merges data from all stations horizontally into a single DataFrame. |

Each station is suffixed by its code (`BER`, `SIO`, `LUZ`, etc.),
and the `global` series represents the national weighted average if `global_meteo=True`.

## Usage example

The example below is implemented in the `SwissMeteo_extraction.ipynb` notebook.
```python
from modules.config import config
from modules.loading import load_meteo_file

# Initialize configuration
cfg = config(freq='d', start_date='2024-01-01', end_date='2024-12-31', global_meteo=True)

# Download MeteoSwiss data
data = load_meteo_file(cfg, is_verbose=True)

# Save locally
data.to_parquet(cfg.save_dir['save directory'])
```

## Expected result

A Pandas DataFrame indexed by time (hours, days, etc.)

Column types:
- `temperature_BER`, `rainfall_GVE`, `humidity_SIO`, ...
- Columns `XX_global*` (if `global_meteo=True`) representing the national weighted average.

## Project structure
```
SwissMeteo_for_Energy/
│
├── .git/                          # Git folder (do not modify)
├── .idea/                         # IDE configuration files
│
├── data/                          # Exported or processed data
│   └── meteo_data.parquet.gz      # Aggregated weather data in Parquet format
│
├── modules/                       # Python source code of the module
│   ├── config.py                  # Main configuration class
│   ├── loading.py                 # Extraction and aggregation functions
│   └── __pycache__/               # Automatic Python cache
│
├── SwissMeteo_extraction.ipynb    # Extraction notebook
└── README.md                      # Project documentation
```

## Data

Data is retrieved via the official public API:
https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn

## License

This project is published under the MIT license.
You are free to modify and redistribute it with attribution to the original author.

## Author

**Jules Courrian**

Developed as part of the TN09 internship at HEIG-VD (Energy Institute)
within the framework of the 2025 update of the EcoDynElec software.