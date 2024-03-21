# QRF

## Setup

### Create virtual environment

#### Mac / Linux
```console
$ python -m venv venv
$ source venv/bin/activate
```
* may be python3 depending on configuration

#### Windows
```console
$ python3 -m venv venv
$ .\venv\Scripts\activate
```

### Install packages

```console
$ pip install -r requirements.txt
```

### Create `credentials.json`

Create a new file `credentials.json` from `credentials.json.template`. Fill in the `password` of the database.

## Running

Every 5 seconds the database will be queried and if triggered, the QRF dispatcher will be called.

```console
$ python src.py
```
