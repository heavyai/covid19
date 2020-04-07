#!/bin/bash

 #this will require that a credentials file exists at ~/.aws/credentials, to avoid leaking creds
export AWS_PROFILE=default

#check that the toml config files exist
function set_prefect_config(){
    fn="$CONFIG_PATH"
    if [ -f ${fn} ]; then
        export PREFECT__USER_CONFIG_PATH="$CONFIG_PATH"
    else 
        echo "Unable to find prefect config file at $CONFIG_PATH, exiting"
        exit 1
    fi
}

usage()
{
  echo "Usage: $0 -s (worldometer|nytimes|c19tp|jhu) -m (now|schedule) -c <config_file_path>"
  exit 2
}

unset

while getopts 's:m:c:' opt;
do
    case "$opt" in
        s)  declare -r choice=$OPTARG
            ;;
        m)  declare -r runmode=$OPTARG
            [ "now" != "$runmode" ] && [ "schedule" != "$runmode" ] && usage
            export PREFECT__RUNMODE=$runmode
            ;;
        c)  declare -r CONFIG_PATH=$OPTARG
            declare -r SCRIPTS_DIR=$(cd "$(dirname "$0")"; pwd)
            set_prefect_config
            ;;
    esac
done

shift "$(($OPTIND -1))"

case $choice in
    nytimes) python3 $SCRIPTS_DIR/nytimes_loader.py 
        ;;
    jhu) python3 $SCRIPTS_DIR/github_jsse_loader.py 
        ;;
    worldometer) python3 $SCRIPTS_DIR/worldometer.py
        ;; 
    c19tp) python3 $SCRIPTS_DIR/c9_tracking_proj_loader.py
        ;;
    *) usage
esac
