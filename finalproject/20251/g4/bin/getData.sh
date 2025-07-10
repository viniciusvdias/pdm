#!/bin/bash
set -euo pipefail

wget https://www.kaggle.com/api/v1/datasets/download/PROPPG-PPG/hourly-weather-surface-brazil-southeast-region

mkdir -p "../dataset"

mv hourly-weather-surface-brazil-southeast-region ../dataset

cd ../dataset

unzip "hourly-weather-surface-brazil-southeast-region"

mkdir -p "info"
mv stations.csv columns_description.csv *.py info/

rm -rf "hourly-weather-surface-brazil-southeast-region"

