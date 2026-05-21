@echo off
REM Watchdog wrapper for the immo bot — exponential backoff on crash
REM Sequence: 1s, 5s, 30s, 5min, capped at 5min
REM Resets to 1s after a clean run of >5 min
cd /d "D:\Users\Illan\Downloads\Claude apps\immo"

REM ── Single-instance guard ─────────────────────────────────────────────────
REM Abort if the single-instance port (47823, same one main.py binds) is busy.
REM This catches a parallel watchdog before it spawns python, giving a clearer
REM message than waiting for main.py to log the OSError and exit. The in-process
REM socket lock in main.py remains the ultimate defense.
netstat -an | findstr /r /c:":47823 .*LISTENING" >nul
if %ERRORLEVEL% EQU 0 (
    echo.
    echo [ABORT] Another bot instance is already running (port 47823 busy^).
    echo Close the existing bot window first, or kill python.exe via Task Manager.
    echo.
    pause
    exit /b 1
)

set BACKOFF=1

:loop
echo [%date% %time%] Starting bot (next backoff if crash: %BACKOFF%s)...
set RUN_START=%TIME%
C:\Python311\python.exe main.py
set EXITCODE=%ERRORLEVEL%
echo [%date% %time%] Bot exited with code %EXITCODE%

REM Compute how long the bot ran in seconds (best-effort)
REM If RUN_START hour matches current hour and minute diff > 5, reset backoff
for /f "tokens=1-3 delims=:," %%a in ("%RUN_START%") do set START_MIN=%%b
for /f "tokens=1-3 delims=:," %%a in ("%TIME%") do set NOW_MIN=%%b
set /a DELTA=%NOW_MIN%-%START_MIN%
if %DELTA% LSS 0 set /a DELTA=%DELTA%+60
if %DELTA% GTR 5 (
    echo [watchdog] Bot ran for %DELTA% minutes -- resetting backoff to 1s
    set BACKOFF=1
)

echo [%date% %time%] Sleeping %BACKOFF%s before restart...
timeout /t %BACKOFF% /nobreak >nul

REM Exponential backoff: 1 -> 5 -> 30 -> 300 (5 min cap)
if %BACKOFF% EQU 1   set BACKOFF=5  & goto loop
if %BACKOFF% EQU 5   set BACKOFF=30 & goto loop
if %BACKOFF% EQU 30  set BACKOFF=300 & goto loop
goto loop
