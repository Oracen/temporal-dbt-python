# temporal-dbt-python

A Temporal DBT Workflow and worker

## Project Description

This is a small learning project to let me experiment with orchestrating self-managed DBT workers within the [Temporal](https://temporal.io/) framework. It will be modelled off both the [officially supported Airflow DBT operator](https://github.com/gocardless/airflow-dbt) and [Tomas Farias's CLI-free version](https://github.com/tomasfarias/airflow-dbt-python). The intent is to build something that leverages the power of Temporal for running distributed workflows as an alternative to standard data engineering tools such as Airflow or Prefect.

This project hooks into the `dbt-core` API to execute workflows, but DBT is really built to run via the command line. Thus, some things like the `write_file` function have been monkeypatched to re-route artifacts that might otherwise fill the memory of the worker instance. It's not a terribly clean solution, but until [Fishtown Analytics finish reworking their APIs](https://github.com/dbt-labs/dbt-core/issues/5527) a quick-and-dirty solution is fine.

This project is experimental and targeted to some POC use cases I have. If you choose to rely on it, understand that it has the following limitations:

- APIs are subject to random arbitrary change
- DBT API support will be limited to what I feel I need
- I don't know if the monkeypatch will behave itself for lots of concurrent workloads in Temporal

While I can see that this limited implementation may not be enough for everyone, I hope it's at least a useful starting point for your own project.

## Example

# Installing

## Requirements

## From this repo

## Usage
