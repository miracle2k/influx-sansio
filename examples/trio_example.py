import trio
from influx_sansio import iter_resp
from influx_sansio.asks import InfluxDBClient
import multio


async def main(chunked=False):
    point = dict(time='2009-11-10T23:00:00Z',
                 measurement='cpu_load_short',
                 tags={'host': 'server01',
                       'region': 'us-west'},
                 fields={'value': 0.64})

    client = InfluxDBClient(db='testdb')

    await client.create_database(db='testdb')
    #await client.write(point)
    return await client.query('SELECT value FROM cpu_load_short', chunked=chunked)


async def main_chunked():
    # https://github.com/theelous3/asks/issues/62
    async for chunk in await main():        
        for result in iter_resp(results):
            print(result)


multio.init('trio')
results = trio.run(main)
for result in iter_resp(results):
    print(result)
