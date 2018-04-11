# Bigdata big screen ETL

## Environment

- Ubuntu 16.04+
- Python 2.7

## Setup

```shell
./setup.sh
```

## Configuration

Edit /etc/bs_bigdata_etl.conf

## Start

```shell
systemctl start bs_bigdata_etl
systemctl status bs_bigdata_etl
```

## Stop

```shell
systemctl stop bs_bigdata_etl
systemctl status bs_bigdata_etl
```

## Update

```shell
python setup.py install
systemctl restart bs_bigdata_etl
systemctl status bs_bigdata_etl
```

