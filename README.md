# covid19
ETL code for the covid19 dashboard at https://www.omnisci.com/demos/covid-19, as well as loaders for multiple data sources

## Dependencies
- prefect - See [https://www.prefect.io/](prefect.io). Great lightweight solution for running the ETL workflow as a proper data pipeline.
- pandas - Installed with pymapd
- pymapd to load data into omnisci
- parsel, for xpath hackery on web sources e.g. worldometer
## Installing deps with conda

```
conda install -c conda-forge prefect pymapd parsel
```

## Configuring the loader script

- If you plan to use the s3 loaders, you should make sure your `AWS_PROFILE` environment variable is set, so that the boto script used by prefect picks up the credentials without leaking them
- If you plan to use omnisci as a destination, you should add a section in the config file like so

```
[[destinations.omnisci]]
host="omniscidb.yourhost.wherever.com"  # change this to the IP or hostname of your omniscidb instance
user="admin" #change if needed
password="HyperInteractive" #change if needed
dbname="covid19" #this must exist already. See omnisci docs on how to create a database.
port="6274" #leave this if you're not sure
protocol="binary" #leave this one too, if you're not sure
```

## Running the loader
The `loaders.sh` shell script is a wrapper around the prefect flows for each source. You can run each source either now or on a schedule. A couple of points to note:
- By default, the flows produce files that are stored locally in a directory named `covid-19-flow-outputs` for each run

```
loaders.sh -s (worldometer|nytimes|c19tp|jhu) -m (now|schedule) -c <config_file_path>"
```
