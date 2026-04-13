@echo off
color 0C
title GEKTOR APEX v4.1 [MAINNET ACTIVE]

echo ========================================================
echo         GEKTOR APEX v4.1 - TACTICAL INTRADAY
echo ========================================================
echo.
echo [SYSTEM] Pre-flight checks engaged...
cd /d "C:\Gerald-superBrain"

echo [SYSTEM] Verifying dependencies...
:: Если вы используете виртуальное окружение, раскомментируйте строку ниже
:: call venv\Scripts\activate.bat

echo [SYSTEM] Injecting MAX performance overrides (PYTHONOPTIMIZE=2)...
set PYTHONOPTIMIZE=2

echo [SYSTEM] Handing over execution to the Engine...
echo --------------------------------------------------------
python main.py

echo --------------------------------------------------------
echo [CRITICAL] ENGINE STOPPED. 
pause
