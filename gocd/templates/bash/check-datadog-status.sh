#!/bin/bash

checks-datadog-monitor-status \
  ${DATADOG_MONITOR_IDS} \
  --skip-check=false
