# Example DAGs

This repository contains example DAGs that can be used "out-of-the-box" using
operators found in the Airflow Plugins organization. These DAGs have a range
of use cases and vary from moving data (see [ETL](https://github.com/airflow-plugins/example_dags/tree/master/etl))
to background system automation that can give your Airflow "super-powers".

## Getting Started
The example DAGs found here can be split into three main categories:

### ETL
These DAGs focus on pulling data from various systems and putting them into
Amazon Redshift, with S3 as a staging store. These represent the simplest
implementation of an "ETL" workflow and can either be used "out-of-the-box"
or extended to add additional custom logic.

### PoC (Proof of Concept)
These DAGs demonstrate simple implementations of custom operators and Airflow
setups. They are typically not "copy-and-paste" DAGs but rather walk through
how something would work.

### System
These DAGs are used on the system administration level and can be thought of
as "meta-DAGs" that maintain various states and configurations within Airflow
itself. In some cases, these DAGs are used in concert with other custom
operators, such as the `rate_limit_reset` DAG.

## Contributions
Contributions of your own DAGs are very welcome. Please see some of the example
DAGs for a sense of general formatting guidelines.

## License
Apache 2.0
