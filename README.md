# Fusion test
## Requirements

- clickhouse_driver (pip3 install clickhouse_driver)

## Creating database entities
Run sql commands from ddl.sql to create necessary entities in db.
## Using
Using app from terminal:
```
usage: task.py [-h] [-ts1 START_DATE] [-ts2 END_DATE] [-ref REFS_REGEX]
               [-act {installs,retention,ltv_avg,ltv}] [-gen USERS_COUNT]

optional arguments:
  -h, --help            show this help message and exit
  -ts1 START_DATE       Start date dd.mm.yyyy
  -ts2 END_DATE         End date dd.mm.yyyy
  -ref REFS_REGEX       Ref regex
  -act {installs,retention,ltv_avg,ltv}
                        Action
  -gen USERS_COUNT      Number of generated users

```

## Generating data and fill database:
```shell script
python3 task.py -gen USERS_COUNT
``` 

## Get metrics
App provide calculate metrics such as:

- installs (action installs)
- retention (action retention)
- ltv - count unique users (action ltv)
- average ltv (action ltv_avg)
### Example get metric:
```shell script
python3 task.py -ts1 05.05.2020 -ts2 10.05.2020 -ref ads.* -act ltv
```

