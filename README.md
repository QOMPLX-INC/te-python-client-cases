# Overview

This repository provides a set of sample cases for TimeEngine Python SDK.

## Forecast

### Command line arguments overview

1. Import CSV and write:

  $ ./forecast/arima.py -w -t 1 --read_back
  create test scenario: 1
  OK: Data are validated

2. Read and validate against CSV:

  $ ./forecast/arima.py -v -t 1
  OK: Data are validated

3. Print info about User/Swimlane:

$ ./forecast/arima.py -i -t 1
  {
      "adm_secret": "...",
      "app": "...",
      "adm": "...",
      "app_secret": "..."
  }

4. Modeling:

  $ ./forecast/arima.py -t 1 --model_country="Germany" --model_p=5 --model_q=3 --model_n=20
  OK: Data are read

  --model_country="Germany" see labels
  --model_p see 'p' param in model descrition
  --model_q see 'q' param in model descrition
  --model_n forecast number

5. Clean data:

  $ ./forecast/arima.py -d -t 1
  clean {u'adm_secret': u'...', u'app': u'...', u'adm': u'...', u'app_secret': u'...'}

# Credits

Data sources:

covid_time_series.csv is a fragment of the following file:
https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv

earthquake*.csv files:
All data collected from this page should be used only for scientific and non-commerical purposes.
Upon use of our data, proper attribution should be given to B.U. KOERI-RETMC
(Boğaziçi University Kandilli Observatory and Earthquake Research Institute - Regional Earthquake-Tsunami Monitoring Center)
in scientific articles and general purpose reports by referencing the KOERI Catalog citation.

# Copyright
Copyright 2020-2021 -- QOMPLX, Inc. -- All Rights Reserved.  No License Granted.
