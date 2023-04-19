import common
import random
from postgissbe import PostGISSBE
from netcdfsbe_tsmdatainpostgis import NetCDFSBE_TSMDataInPostGIS
from timeseries import TimeSeries
from abc import ABC, abstractmethod
from pgconnectioninfo import PGConnectionInfo
import json
import sys


class TestBase(ABC):
    def __init__(self, verbose, config, storage_backends):
        self._verbose = verbose
        self._config = config
        self._storage_backends = storage_backends
        self._stats = {sbe.descr(): {} for sbe in storage_backends}

    @abstractmethod
    def descr(self):
        """Get description of test."""

    @abstractmethod
    def _execute(self):
        """Execute test."""

    def execute(self, stats):
        """Execute test, accumulating stats."""
        self._execute()
        stats[self.descr()] = self._stats

    def _reg_stats(self, sbe, stats_key, stats_val):
        """Register stats (typically elapsed secs for an operation) for a storage backend."""
        self._stats[sbe.descr()][stats_key] = stats_val


class Reset(TestBase):
    def __init__(self, verbose, config, storage_backends, tss):
        super().__init__(verbose, config, storage_backends)
        self._tss = tss

    def descr(self):
        return 'reset storage backends with {} time series'.format(len(self._tss))

    def _execute(self):
        for sbe in self._storage_backends:
            start_secs = common.now_secs()
            sbe.reset(self._tss)
            self._reg_stats(sbe, 'total secs', common.elapsed_secs(start_secs))


class FillStorage(TestBase):
    def __init__(self, verbose, config, storage_backends, tss, curr_time):
        super().__init__(verbose, config, storage_backends)
        self._tss = tss
        self._curr_time = curr_time

    def descr(self):
        return 'fill storage with observations'

    def _execute(self):
        # fill each time series with observations using the entire accessible capacity
        # ([curr_time - max_age, curr_time])
        ts_data = []
        from_time, to_time = self._curr_time - self._config['max_age'], self._curr_time
        for ts in self._tss:
            times, obs = ts.create_observations(from_time, to_time)
            ts_data.append((ts, times, obs))

        # store the time series in each backend
        for sbe in self._storage_backends:
            start_secs = common.now_secs()
            for td in ts_data:
                sbe.set_obs(td[0], td[1], td[2])
            self._reg_stats(sbe, 'total secs', common.elapsed_secs(start_secs))


class GetObsAll(TestBase):
    def __init__(self, verbose, config, storage_backends, curr_time):
        super().__init__(verbose, config, storage_backends)
        self._curr_time = curr_time

    def descr(self):
        return 'get all observations in the storage'

    def _execute(self):
        # retrieve all observations for all time series in time range
        # [curr_time - max_age, curr_time]

        # retrieve from each backend
        from_time, to_time = self._curr_time - self._config['max_age'], self._curr_time
        for sbe in self._storage_backends:
            start_secs = common.now_secs()
            sbe.get_obs_all(from_time, to_time)  # don't use return value
            self._reg_stats(sbe, 'total secs', common.elapsed_secs(start_secs))


class TsTester:
    """Tests/compares different time series storage backends wrt. performance."""

    def __init__(self, verbose, config):
        self._verbose = verbose
        self._config = config
        pg_host = common.get_env_var('PGHOST', 'localhost')
        pg_port = common.get_env_var('PGPORT', '5432')
        pg_user = common.get_env_var('PGUSER', 'postgres')
        pg_password = common.get_env_var('PGPASSWORD', 'mysecretpassword')
        self._storage_backends = [  # storage backends to test/compare
            PostGISSBE(
                verbose,
                PGConnectionInfo(
                    pg_host, pg_port, pg_user, pg_password,
                    common.get_env_var('PGDBNAME_POSTGIS', 'esoh_postgis')
                )
            ),
            NetCDFSBE_TSMDataInPostGIS(
                verbose,
                PGConnectionInfo(
                    pg_host, pg_port, pg_user, pg_password,
                    common.get_env_var('PGDBNAME_NETCDF', 'esoh_netcdf')
                ),
                common.get_env_var('NCDIR', 'ncdir')
            ),
        ]

    def execute(self):
        """Execute overall test/comparison."""

        test_stats = {}

        tss = create_time_series(self._verbose, self._config)

        Reset(self._verbose, self._config, self._storage_backends, tss).execute(test_stats)

        curr_time = int(common.now_secs())

        FillStorage(self._verbose, self._config, self._storage_backends, tss, curr_time).execute(
            test_stats)

        GetObsAll(self._verbose, self._config, self._storage_backends, curr_time).execute(
            test_stats)

        # TODO: replace FillStorage with InsertObs(curr_time - cfg.max_age, curr_time) (still using sbe.set_obs())
        # TODO: replace AppendNewObservations with InsertObs(curr_time, curr_time + DELTA) (but now using sbe.add_obs())

        # TODO: more tests (subclasses of TestBase):
        # - AppendNewObservations
        # - GetObsInCircle
        # - GetObsInPolygon
        # - GetObsFromStations
        # - GetObsFromParams
        # - GetObsFromStationParams
        # - ...

        stats = {
            'overall': {'key1': 'val1', 'key2': 'val2'},  # TODO: add some useful overall info here
            'tests': test_stats,
        }

        print(json.dumps(stats, indent=4))


def create_time_series(verbose, config):
    """Generate a set of time series identified by station/param combos.
    The configuration is used for randomizing ...
        ... the time resolution for each time series, and
        ... the set of params for each station.

    Returns a list of TimeSeries objects.
    """

    nstations = config['nstations']

    time_res = config['time_res']
    time_res = list({int(k): v for k, v in time_res.items()}.items())

    min_params = config['params']['min']
    max_params = config['params']['max']

    param_ids = list(map(lambda i: 'param_{}'.format(i), [i for i in range(max_params)]))

    min_lat = config['bbox']['min_lat']
    max_lat = config['bbox']['max_lat']
    if (min_lat < -90) or (min_lat >= max_lat) or (max_lat > 90):
        raise Exception('invalid latitude range in bounding box: [{}, {}]'.format(
            min_lat, max_lat))

    min_lon = config['bbox']['min_lon']
    max_lon = config['bbox']['max_lon']
    if (min_lon < -180) or (min_lon >= max_lon) or (max_lon > 180):
        raise Exception('invalid longitude range in bounding box: [{}, {}]'.format(
            min_lon, max_lon))

    tss = []
    used_locs = set([])  # lat,lon locations used so far

    def create_new_loc():
        """Return a unique lat,lon tuple within the bounding box
        ([min_lat, max_lat] X [min_lon, max_lon]).
        """
        while True:
            lat = min_lat + random.random() * (max_lat - min_lat)
            lon = min_lon + random.random() * (max_lon - min_lon)
            if not (lat, lon) in used_locs:
                break
        used_locs.add((lat, lon))
        return lat, lon

    def create_ts_other_metadata():
        """Return dict of per time series metadata."""
        return config['ts_other_metadata']  # ### for now; eventually randomize?

    def create_obs_metadata():
        """Return dict of per observation metadata."""
        return config['obs_metadata']  # ### for now; eventually randomize?

    for s in range(nstations):
        if verbose:
            print('next station: {}'.format(s), file=sys.stderr)

        lat, lon = create_new_loc()
        random.shuffle(param_ids)

        for p in range(random.randint(min_params, max_params)):
            ts_other_mdata = create_ts_other_metadata()
            obs_mdata = create_obs_metadata()

            ts = TimeSeries(
                verbose, 'station_{}'.format(s), lat, lon,
                param_ids[p], common.select_weighted_value(time_res),
                ts_other_mdata, obs_mdata
            )
            if verbose:
                print('new ts (s = {}, p = {}): {}'.format(s, p, vars(ts)), file=sys.stderr)

            tss.append(ts)

    return tss
