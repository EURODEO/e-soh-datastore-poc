from storagebackend import StorageBackend
import common
from pgopbackend import Psycopg2BE, PsqlBE


class PostGISSBE(StorageBackend):
    """A storage backend that uses a PostGIS database for storage of observations,
    per observation metadata, and per time series metadata."""

    def __init__(self, verbose, pg_conn_info):
        super().__init__(verbose, 'PostGIS')

        self._conn_info = pg_conn_info

        # recreate database from scratch
        self.__drop_database()
        self.__create_database()

        # create a database operation executor backend
        if common.get_env_var('PGOPBACKEND', 'psycopg2') == 'psycopg2':
            self._pgopbe = Psycopg2BE(verbose, self._conn_info)
        else:
            self._pgopbe = PsqlBE(verbose, self._conn_info)

        # install the postgis extension
        self.__install_postgis_extension()

    def __drop_database(self):
        """Drop any existing database named self._conn_info.dbname()."""

        if self._verbose:
            print('dropping database {} ... '.format(self._conn_info.dbname()), end='', flush=True)
        common.exec_command([
            'dropdb', '-w', '-f', '--if-exists', '-h', self._conn_info.host(),
            '-p', self._conn_info.port(), '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', flush=True)

    def __create_database(self):
        """Create database named self._conn_info.dbname()."""

        if self._verbose:
            print('creating database {} ... '.format(self._conn_info.dbname()), end='', flush=True)
        common.exec_command([
            'createdb', '-w', '-h', self._conn_info.host(), '-p', self._conn_info.port(),
            '-U', self._conn_info.user(), self._conn_info.dbname()])
        if self._verbose:
            print('done', flush=True)

    def __install_postgis_extension(self):
        """Install the PostGIS extension."""

        if self._verbose:
            print('installing PostGIS extension ... ', end='', flush=True)
        self._pgopbe.execute('CREATE EXTENSION postgis')
        if self._verbose:
            print('done', flush=True)

    def reset(self, tss):
        """See documentation in base class."""

        if self._verbose:
            print('\nresetting PostGIS SBE with {} time series ... TODO'.format(len(tss)))
            print('---BEGIN tss -------')
            for ts in tss:
                print(ts.__dict__)
            print('---END tss -------')

        # assume at this point that self._conn_info.dbname() exists, but not that it is
        # empty, so first step is to drop schema (all tables and indexes):
        self._pgopbe.execute('DROP TABLE IF EXISTS ts')
        # self._pgopbe.execute('DROP INDEX IF EXISTS ...')  # TODO?

        # TODO:
        # - create schema (including a time series table and an observation table) based on info
        #   in tss
        # ------------>

        # CREATE TABLE ts(
        # station_name: VARCHAR,
        # param_name: VARCHAR,
        # lat: DOUBLE,
        # lon: DOUBLE,
        # mdata: JSONB (dict));
        #
        # PK: (station_name, param_name)
        # UNIQUE: (lat, lon)
        #

        # - insert rows in time series table
        # ------------>
        for ts in tss:
            pass
            # INSERT INTO ts VALUES (...)

        # - create indexes
        # TODO

        # ensure that PostGIS is enabled to perform quick geo searches for this case
        # (tss inside circle and polygon)
        # TODO

    def set_obs(self, ts, times, obs):
        """See documentation in base class."""

        if self._verbose:
            print('\nsetting observations in PostGIS SBE for time series ... TODO')
            print('    ts: {}\n    times: ({} values), obs: ({} values)'.format(
                ts.__dict__, len(times), len(obs)))

        # TODO:
        # - insert rows in observation table

    def add_obs(self, ts, times, obs):
        """See documentation in base class."""
        # TODO

    def get_obs(self, tss, from_time, to_time):
        """See documentation in base class."""
        # TODO

    def get_tss_in_circle(self, lat, lon, radius):
        """See documentation in base class."""
        # TODO
