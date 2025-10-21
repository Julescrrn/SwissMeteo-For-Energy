"""
Configuration module for MeteoSwiss data extraction and processing.

This module defines the `config` class, which centralizes all
user-defined parameters needed for downloading and processing
meteorological data. It includes attributes for frequency,
time range, global weighting behavior, and output directories.
"""
import pandas as pd

class config:
    """
    Configuration object for MeteoSwiss data operations.

    This class stores and manages configuration parameters such as
    temporal range, data frequency, global aggregation mode, and save paths.
    It ensures consistent behavior across different data download and
    analysis modules.

    Parameters
    ----------
    freq : str, optional
        Frequency code ('h', 'd', 'm', 'y') used for MeteoSwiss data.
    start_date : str or pandas.Timestamp, optional
        Start date for data retrieval (default: 01 January 2024).
    end_date : str or pandas.Timestamp, optional
        End date for data retrieval (default: 31 December 2024).
    global_meteo : bool, optional
        Whether to compute population-weighted global meteorological indicators.
    save_dir : dict or str, optional
        Output directory or dictionary of paths where data will be saved.

    Attributes
    ----------
    freq : str
        Temporal frequency of data.
    global_meteo : bool
        Indicates whether to compute a global weighted dataset.
    custom_dates : bool
        True if the user provided custom start or end dates.
    start_date : pandas.Timestamp
        Start of the selected period.
    end_date : pandas.Timestamp
        End of the selected period.
    save_dir : dict
        Dictionary containing save paths for data exports.

    Methods
    -------
    set_freq(freq)
        Set or update the frequency parameter.
    set_period(start_date, end_date)
        Define a new time period for analysis.
    set_path(source, path)
        Modify the save directory for a specific data source.
    set_global_meteo(global_meteo)
        Enable or disable global meteorological aggregation.
    get_path(source)
        Retrieve the save directory for a specific source.
    get_summary()
        Return a dictionary summarizing the configuration.
    """

    def __init__(self, freq=None, start_date=None, end_date=None, global_meteo=False, save_dir=None):
        self.freq = freq
        self.global_meteo = global_meteo
        self.custom_dates = start_date is not None or end_date is not None

        default_start = pd.to_datetime("2016-01-01")
        self.start_date = pd.to_datetime(start_date) if start_date else default_start

        default_end = pd.to_datetime("2024-12-31")
        self.end_date = pd.to_datetime(end_date) if end_date else default_end

        default_paths = {
            "save directory": './data/swissmeteo.parquet.gz',
        }
        self.save_dir = save_dir if save_dir is not None else default_paths

    def set_freq(self, freq):
        """
        Define or modify the frequency parameter.

        Parameters
        ----------
        freq : str
            Frequency code ('h', 'd', 'm', 'y').
        """
        self.freq = freq.lower()

    def set_period(self, start_date, end_date):
        """
        Set a new analysis period.

        Parameters
        ----------
        start_date : str or pandas.Timestamp
            Start of the new period.
        end_date : str or pandas.Timestamp
            End of the new period.
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)

    def set_path(self, source, path):
        """
        Update the save directory for a specific data source.

        Parameters
        ----------
        source : str
            Key corresponding to the data source (e.g., "save directory").
        path : str
            File path where the data should be saved.

        Raises
        ------
        ValueError
            If the provided source key is not known.
        """
        if source in self.save_dir:
            self.save_dir[source] = path
        else:
            raise ValueError(f"Unknown data source: {source}")

    def set_global_meteo(self, global_meteo):
        """
        Enable or disable global meteorological data aggregation.

        Parameters
        ----------
        global_meteo : bool
            True to enable, False to disable.

        Raises
        ------
        ValueError
            If the argument is not a boolean.
        """
        if global_meteo not in [True, False]:
            raise ValueError("global_meteo must be True or False")
        else:
            self.global_meteo = global_meteo

    def get_path(self, source):
        """
        Retrieve the save directory for a given data source.

        Parameters
        ----------
        source : str
            Key name for the data source.

        Returns
        -------
        str or None
            Path string if found, otherwise None.
        """
        return self.save_dir.get(source, None)

    def get_summary(self):
        """
        Return a summary of the configuration parameters.

        Returns
        -------
        dict
            Dictionary containing frequency, date range, global flag, and save directory.
        """
        return {
            "global_meteo": self.global_meteo,
            "freq": self.freq,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "save_dir": self.save_dir
        }

    def __repr__(self):
        """
        Return a concise representation of the configuration object.

        Returns
        -------
        str
            String representation for debugging or logging.
        """
        return f"<Config {self.get_summary()}>"
