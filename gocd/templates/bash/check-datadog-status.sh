#!/bin/bash

/devinfra/scripts/checks/datadog/monitor_status.py \
  ${DATADOG_MONITOR_IDS} \
  --skip-check=false
