@echo off
REM Move to the project folder
cd /d C:\MCSERVERS\QRF-Triggers

REM Create venv if it doesn't exist
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate venv
call venv\Scripts\activate.bat

REM Install requirements (only installs missing packages)
echo Installing/updating dependencies...
pip install -r requirements.txt

REM Run the script
python src.py --initial-newer-than "2025-07-27 8:00:00" --saveload umaine2025am --wid 134

pause