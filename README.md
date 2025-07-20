# QRF

## Setup

### Create virtual environment

#### Mac / Linux
```console
$ python3 -m venv venv
$ source venv/bin/activate
```
#### Windows
```console
$ python -m venv venv
$ .\venv\Scripts\activate
```
* may be python3 depending on configuration

### Install packages

```console
$ pip install -r requirements.txt
```

### Create `credentials.json`

Create a new file `credentials.json` from `credentials.json.template`. Fill in the `password` of the database.

## Running

Every 5 seconds the database will be queried and if triggered, the QRF dispatcher will be called.

```console
$ python scriptname.py --initial-newer-than "YYYY-MM-DD hh:mm:ss" --saveload filename --wid ###
```

## Example Batch File

Create a new file in Notepad and save-as "startqrf.bat" (with quotes included + all file types) with the following content:

```console
@echo off
REM Change to the directory where the script and config files are located
cd /d C:\MCSERVERS\QRF-Triggers

REM Activate virtual environment (adjust path to your actual venv if needed)
call venv\Scripts\activate.bat

REM Run the script with arguments
python scriptname.py --initial-newer-than "YYYY-MM-DD hh:mm:ss" --saveload filename --wid ###

REM Pause so the window stays open
pause
```
