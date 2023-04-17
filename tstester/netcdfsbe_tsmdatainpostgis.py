from storagebackend import StorageBackend
from postgissbe import PostGISSBE
import shutil


class NetCDFSBE_TSMDataInPostGIS(StorageBackend):
    """A storage backend that uses netCDF files on the local file system for storage
    of observations and per observation metadata, and a PostGIS database for keeping per time
    series metadata.

    There will be one netCDF file per time series (station/param combo). Each such file will
    contain observations as well as all metadata; both per observation and per time series.
    The rationale for keeping per time series metadata in PostGIS is to provide a faster search
    for relevant time series. The actual observations are then be retrieved from the netCDF
    files in a second step.

    Files will be organized like this under self._nc_dir:

        station_id1/
           param_id1/
              data.nc
           param_id2/
              data.nc
           ...
        station_id2/
           ...
    """

    # TODO: pass directory in which to keep files to __init__ --->
    def __init__(self, verbose, pg_conn_info, nc_dir):
        super().__init__(verbose, 'netCDF/time series metadata in PostGIS')
        self._pgsbe = PostGISSBE(verbose, pg_conn_info)  # for keeping per time series metadata
        self._nc_dir = nc_dir  # directory under which to keep the netCDF files

    def reset(self, tss):
        """See documentation in base class."""
        if self._verbose:
            print('\nresetting NetCDF SBE with {} time series ... TODO'.format(len(tss)))

        self._pgsbe.reset(tss)

        # wipe any existing directory
        shutil.rmtree(self._nc_dir, ignore_errors=True)

        # TODO:
        # - create files with all ts-specific metadata, but with no observations

    def set_obs(self, ts, times, obs):
        """See documentation in base class."""
        # TODO:
        # - replace contents of times and obs variables in file

    def add_obs(self, ts, times, obs):
        """See documentation in base class."""
        # TODO

    def get_obs(self, tss, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_tss_in_circle(self, lat, lon, radius):
        """See documentation in base class."""
        # TODO
