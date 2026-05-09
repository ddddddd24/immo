@echo off
REM Watchdog wrapper for the dashboard — exponential backoff on crash
cd /d "D:\Users\Illan\Downloads\Claude apps\immo"
set BACKOFF=1

:loop
echo [%date% %time%] Starting dashboard (next backoff if crash: %BACKOFF%s)...
set RUN_START=%TIME%
C:\Python311\python.exe dashboard.py
set EXITCODE=%ERRORLEVEL%
echo [%date% %time%] Dashboard exited with code %EXITCODE%

for /f "tokens=1-3 delims=:," %%a in ("%RUN_START%") do set START_MIN=%%b
for /f "tokens=1-3 delims=:," %%a in ("%TIME%") do set NOW_MIN=%%b
set /a DELTA=%NOW_MIN%-%START_MIN%
if %DELTA% LSS 0 set /a DELTA=%DELTA%+60
if %DELTA% GTR 5 (
    echo [watchdog] Dashboard ran for %DELTA% minutes -- resetting backoff to 1s
    set BACKOFF=1
)

echo [%date% %time%] Sleeping %BACKOFF%s before restart...
timeout /t %BACKOFF% /nobreak >nul

if %BACKOFF% EQU 1   set BACKOFF=5  & goto loop
if %BACKOFF% EQU 5   set BACKOFF=30 & goto loop
if %BACKOFF% EQU 30  set BACKOFF=300 & goto loop
goto loop
