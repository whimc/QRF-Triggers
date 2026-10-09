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

REM Run the script. --wid lists the camp's build worlds (133 = Umaine25am, 134 = Umaine25pm);
REM both camps were treated as build worlds during the 2025 camp.
python src.py --initial-newer-than "2025-07-27 12:30:00" --saveload umaine2025pm --wid 133 134

pause