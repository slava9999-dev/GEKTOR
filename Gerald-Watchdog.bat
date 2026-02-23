@echo off
chcp 65001 >nul 2>&1
TITLE Gerald-SuperBrain Watchdog
:loop
echo [%date% %time%] Starting Gerald Bridge Daemon...
python bridge/bridge_daemon.py
echo [%date% %time%] WARNING: Daemon stopped. Restarting in 5 seconds...
timeout /t 5
goto loop
