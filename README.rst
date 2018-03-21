influx-sansio
=============

Python client for InfluxDB following the `SansIO`_ principle.

InfluxDB is an open-source distributed time series database. Find more
about InfluxDB at http://influxdata.com/


Installation
------------

To install the latest release:

.. code:: bash

    $ pip install influx-sansio

The library is still in beta, so you may also want to install the latest version from
the development branch:

.. code:: bash

    $ pip install git+https://github.com/miracle2k/influx-sansio@dev


Dependencies
~~~~~~~~~~~~

The library supports Python 3.5+.

There is one optional third-party library dependency: 
|pandas|_ for (optional) ``DataFrame`` reading/writing support.

For the concrete IO implementations, there are aditional dependencies.

.. |pandas| replace:: ``pandas``
.. _pandas: https://github.com/pandas-dev/pandas
.. _`SansIO`: https://sans-io.readthedocs.io/



Usage
-----

The module has these parts:

- Low-level utilities that implement generating and parsing the InfluxDB 
  line protocol (for writing data), and some helpers for generating queries.

  This is Sans-IO, and you can use this to implement your own client.

- An abstract base class that provides a easy to use `client` interface,
  which lets you do `client.query()` or `client.write()` calls.

- Concrete implementations of this base class for various IO backends,
  currently the `asks` library which supports both `trio` and `curio`.


Sans-IO (low-level utilities)
~~~~~~~~~~~

See the modules `influx_sansio.serialization` and `influx_sansio.http`.


Client
~~~~~~



.. code:: python

    import asyncio
    import trio
    from influx_sansio.asks import InfluxDBClient

    point = dict(time='2009-11-10T23:00:00Z',
                 measurement='cpu_load_short',
                 tags={'host': 'server01',
                       'region': 'us-west'},
                 fields={'value': 0.64})

    client = InfluxDBClient(db='testdb')

    coros = [client.create_database(db='testdb'),
             client.write(point),
             client.query('SELECT value FROM cpu_load_short')]

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(asyncio.gather(*coros))
    for result in results:
        print(result)


Writing data
~~~~~~~~~~~~

Input data can be:

1. A string properly formatted in InfluxDB's line protocol
2. A dictionary containing the following keys: ``measurement``, ``time``, ``tags``, ``fields``
3. A Pandas ``DataFrame`` with a ``DatetimeIndex``
4. An iterable of one of the above

Input data in formats 2-4 are parsed into the `line protocol`_ before being written to InfluxDB.
Beware that serialization is not highly optimized (cythonization PRs are welcome!) and may become
a bottleneck depending on your application.

The ``write`` method returns ``True`` when successful and raises an
``InfluxDBError`` otherwise.

.. _`line protocol`: https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_reference/


Writing dictionary-like objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We accept any dictionary-like object (mapping) as input.
However, that dictionary must be properly formatted and contain the
following keys:

1) **measurement**: Optional. Must be a string-like object. If
   omitted, must be specified when calling ``InfluxDBClient.write``
   by passing a ``measurement`` argument.
2) **time**: Optional. The value can be ``datetime.datetime``,
   date-like string (e.g., ``2017-01-01``, ``2009-11-10T23:00:00Z``) or
   anything else that can be parsed by Pandas' |Timestamp|_ class initializer.
3) **tags**: Optional. This must contain another mapping of field
   names and values. Both tag keys and values should be strings.
4) **fields**: Mandatory. This must contain another mapping of field
   names and values. Field keys should be strings. Field values can be
   ``float``, ``int``, ``str``, or ``bool`` or any equivalent type (e.g. Numpy types).

.. |Timestamp| replace:: ``Timestamp``
.. _Timestamp: https://pandas.pydata.org/pandas-docs/stable/timeseries.html


Any fields other then the above will be ignored when writing data to
InfluxDB.

A typical dictionary-like point would look something like the following:

.. code:: python

    {'time': '2009-11-10T23:00:00Z',
    'measurement': 'cpu_load_short',
    'tags': {'host': 'server01', 'region': 'us-west'},
    'fields': {'value1': 0.64, 'value2': True, 'value3': 10}}


Writing DataFrames
^^^^^^^^^^^^^^^^^^

We also accept Pandas dataframes as input. The only requirements
for the dataframe is that the index **must** be of type
``DatetimeIndex``. Also, any column whose ``dtype`` is ``object`` will
be converted to a string representation.

A typical dataframe input should look something like the following:

.. code:: text

                                           LUY       BEM       AJW tag
    2017-06-24 08:45:17.929097+00:00  2.545409  5.173134  5.532397   B
    2017-06-24 10:15:17.929097+00:00 -0.306673 -1.132941 -2.130625   E
    2017-06-24 11:45:17.929097+00:00  0.894738 -0.561979 -1.487940   B
    2017-06-24 13:15:17.929097+00:00 -1.799512 -1.722805 -2.308823   D
    2017-06-24 14:45:17.929097+00:00  0.390137 -0.016709 -0.667895   E

The measurement name must be specified with the ``measurement`` argument
when calling ``InfluxDBClient.write``. Additional tags can also be
passed using arbitrary keyword arguments.

**Example:**

.. code:: python

    client = InfluxDBClient(db='testdb')
    client.write(df, measurement='prices', tag_columns=['tag'], asset_class='equities')

In the example above, ``df`` is the dataframe we are trying to write to
InfluxDB and ``measurement`` is the measurement we are writing to.

``tag_columns`` is in an optional iterable telling which of the
dataframe columns should be parsed as tag values. If ``tag_columns`` is
not explicitly passed, all columns in the dataframe will be treated as
InfluxDB field values.

Any other keyword arguments passed to ``InfluxDBClient.write`` are
treated as extra tags which will be attached to the data being written
to InfluxDB. Any string which is a valid `InfluxDB identifier`_ and
valid `Python identifier`_ can be used as an extra tag key (with the
exception of they strings ``data``, ``measurement`` and ``tag_columns``).

See ``InfluxDBClient.write`` docstring for details.

.. _`InfluxDB identifier`: https://docs.influxdata.com/influxdb/latest/query_language/spec/#identifiers
.. _`Python identifier`: https://docs.python.org/3/reference/lexical_analysis.html#identifiers


Querying data
~~~~~~~~~~~~~

Querying data is as simple as passing an InfluxDB query string to
``InfluxDBClient.query``:

.. code:: python

    client.query('SELECT myfield FROM mymeasurement')

The result (in ``blocking`` and ``async`` modes) is a dictionary
containing the parsed JSON data returned by the InfluxDB `HTTP API`_:

.. _`HTTP API`: https://docs.influxdata.com/influxdb/latest/guides/querying_data/#querying-data-using-the-http-api

.. code:: python

    {'results': [{'series': [{'columns': ['time', 'Price', 'Volume'],
         'name': 'mymeasurement',
         'values': [[1491963424224703000, 5783, 100],
          [1491963424375146000, 5783, 200],
          [1491963428374895000, 5783, 100],
          [1491963429645478000, 5783, 1100],
          [1491963429655289000, 5783, 100],
          [1491963437084443000, 5783, 100],
          [1491963442274656000, 5783, 900],
          [1491963442274657000, 5782, 5500],
          [1491963442274658000, 5781, 3200],
          [1491963442314710000, 5782, 100]]}],
       'statement_id': 0}]}


Retrieving DataFrames
^^^^^^^^^^^^^^^^^^^^^

When the client is in ``dataframe`` mode, ``InfluxDBClient.query`` will
return a Pandas ``DataFrame``:


.. code:: text

                                      Price  Volume
    2017-04-12 02:17:04.224703+00:00   5783     100
    2017-04-12 02:17:04.375146+00:00   5783     200
    2017-04-12 02:17:08.374895+00:00   5783     100
    2017-04-12 02:17:09.645478+00:00   5783    1100
    2017-04-12 02:17:09.655289+00:00   5783     100
    2017-04-12 02:17:17.084443+00:00   5783     100
    2017-04-12 02:17:22.274656+00:00   5783     900
    2017-04-12 02:17:22.274657+00:00   5782    5500
    2017-04-12 02:17:22.274658+00:00   5781    3200
    2017-04-12 02:17:22.314710+00:00   5782     100

Mode can be chosen not only during object instantiation but also by
simply |changing_mode|_.


.. |changing_mode| replace:: changing the ``mode`` attribute
.. _changing_mode: #switching-modes


Chunked responses
^^^^^^^^^^^^^^^^^

The library supports InfluxDB chunked queries. Passing ``chunked=True`` when calling
``InfluxDBClient.query``, returns an AsyncGenerator object, which can asynchronously
iterated. Using chunked requests allows response processing to be partially done before
the full response is retrieved, reducing overall query time.

.. code:: python

    chunks = await client.query("SELECT * FROM mymeasurement", chunked=True)
    async for chunk in chunks:
        # do something
        await process_chunk(...)


For Python 3.5, this relies on the async_generator (https://github.com/python-trio/async_generator)
library.


Iterating responses
^^^^^^^^^^^^^^^^^^^

``InfluxDBClient.query`` returns a parsed JSON response from InfluxDB. In order to easily 
iterate over that JSON response point by point, we provide the ``iter_resp`` generator:

.. code:: python

    from influx_sansio import iter_resp

    r = client.query('SELECT * from h2o_quality LIMIT 10')
    for i in iter_resp(r):
        print(i)

.. code:: text

    [1439856000000000000, 41, 'coyote_creek', '1']
    [1439856000000000000, 99, 'santa_monica', '2']
    [1439856360000000000, 11, 'coyote_creek', '3']
    [1439856360000000000, 56, 'santa_monica', '2']
    [1439856720000000000, 65, 'santa_monica', '3']

``iter_resp`` can also be used with chunked responses:

.. code:: python

    chunks = await client.query('SELECT * from h2o_quality', chunked=True)
    async for chunk in chunks:
        for point in iter_resp(chunk):
            # do something

By default, ``iter_resp`` yields a plain list of values without doing any expensive parsing.
However, in case a specific format is needed, an optional ``parser`` argument can be passed.
``parser`` is a function that takes the raw value list for each data point and an additional
metadata dictionary containing all or a subset of the following:
``{'columns', 'name', 'tags', 'statement_id'}``.


.. code:: python

    r = await client.query('SELECT * from h2o_quality LIMIT 5')
    for i in iter_resp(r, lambda x, meta: dict(zip(meta['columns'], x))):
        print(i)

.. code:: text

    {'time': 1439856000000000000, 'index': 41, 'location': 'coyote_creek', 'randtag': '1'}
    {'time': 1439856000000000000, 'index': 99, 'location': 'santa_monica', 'randtag': '2'}
    {'time': 1439856360000000000, 'index': 11, 'location': 'coyote_creek', 'randtag': '3'}
    {'time': 1439856360000000000, 'index': 56, 'location': 'santa_monica', 'randtag': '2'}
    {'time': 1439856720000000000, 'index': 65, 'location': 'santa_monica', 'randtag': '3'}


Query patterns
^^^^^^^^^^^^^^

The library provides a wrapping mechanism around ``InfluxDBClient.query`` in
order to provide convenient access to commonly used query patterns.

Query patterns are query strings containing optional named "replacement fields"
surrounded by curly braces ``{}``, just as in |str_format|_.
Replacement field values are defined by keyword arguments when calling the method
associated with the query pattern. Differently from plain |str_format|, positional
arguments are also supported and can be mixed with keyword arguments.

Built-in query patterns are defined on the class.
Users can also dynamically define additional query patterns by using
the |set_qp|_ helper function.
User-defined query patterns have the disadvantage of not being shown for
auto-completion in IDEs such as Pycharm.
However, they do show up in dynamic environments such as Jupyter.
If you have a query pattern that you think will used by many people and should be built-in,
please submit a PR.

Built-in query pattern examples:

.. code:: python

    client.create_database(db='foo')   # CREATE DATABASE {db}
    client.drop_measurement('bar')     # DROP MEASUREMENT {measurement}'
    client.show_users()                # SHOW USERS

    # Positional and keyword arguments can be mixed
    client.show_tag_values_from('bar', key='spam')  # SHOW TAG VALUES FROM {measurement} WITH key = "{key}"

Please refer to InfluxDB documentation_ for further query-related information.

.. _documentation: https://docs.influxdata.com/influxdb/latest/query_language/
.. |str_format| replace:: ``str_format()``
.. _str_format: https://docs.python.org/3/library/string.html#formatstrings


Other functionality
~~~~~~~~~~~~~~~~~~~

Authentication
^^^^^^^^^^^^^^

The library supports basic HTTP authenticatio. Simply pass ``username`` and ``password`` 
when instantiating ``InfluxDBClient``:

.. code:: python

    client = InfluxDBClient(username='user', password='pass)


Unix domain sockets
^^^^^^^^^^^^^^^^^^^

If your InfluxDB server uses UNIX domain sockets you can use ``unix_socket``
when instantiating ``InfluxDBClient``:

.. code:: python

    client = InfluxDBClient(unix_socket='/path/to/socket')


HTTPS/SSL
^^^^^^^^^

The library uses HTTP by default, but HTTPS can be used by passing ``ssl=True``
when instantiating ``InfluxDBClient``:


.. code:: python

    client = InfluxDBClient(host='my.host.io', ssl=True)


Database selection
^^^^^^^^^^^^^^^^^^

After the instantiation of the ``InfluxDBClient`` object, database
can be switched by changing the ``db`` attribute:

.. code:: python

    client = InfluxDBClient(db='db1')
    client.db = 'db2'

Beware that differently from some NoSQL databases (such as MongoDB),
InfluxDB requires that a databases is explicitly created (by using the
|CREATE_DATABASE|_ query) before doing any operations on it.

.. |CREATE_DATABASE| replace:: ``CREATE DATABASE``
.. _`CREATE_DATABASE`: https://docs.influxdata.com/influxdb/latest/query_language/database_management/#create-database


Implementation
--------------

Since InfluxDB exposes all its functionality through an `HTTP
API <https://docs.influxdata.com/influxdb/latest/tools/api/>`__,
``InfluxDBClient`` tries to be nothing more than a thin and simple
wrapper around that API.

The InfluxDB HTTP API exposes exactly three endpoints/functions:
``ping``, ``write`` and ``query``.

``InfluxDBClient`` merely wraps these three functions and provides
some parsing functionality for generating line protocol data (when
writing) and parsing JSON responses (when querying).

Additionally,
`partials <https://en.wikipedia.org/wiki/Partial_application>`__ are
used in order to provide convenient access to commonly used query
patterns. See the `Query patterns <#query-patterns>`__
section for details.


Credits
-------

Forked from `aioinflux <https://github.com/plugaai/aioinflux>`_.
