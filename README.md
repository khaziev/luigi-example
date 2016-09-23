# luigi-example

## Overview

This is a short example that can help to understand the basics of luigi. The programs make a pipeline that includes following steps:
1. Download permit data from City of Chicago website
2. Clean the data
3. Execute 3 simple task to analyze the dataset

## Execution

python -m luigi --module permits_luigi RunAll --local-scheduler

# Note
--local-scheduler is required only if luigy daemon has not been started
