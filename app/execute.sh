#!/bin/bash


APP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$( dirname "$APP_DIR" )" && pwd )"


python3 $APP_DIR"/src/reader.py"  $ROOT_DIR"/.database.txt"   $ROOT_DIR"/data/historical_data1_2012.zip"  
python3 $APP_DIR"/src/tester.py"  $ROOT_DIR"/.database.txt"   $ROOT_DIR"/data/historical_data1_2012.zip"  
