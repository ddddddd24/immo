"""Pause the scrapers when a fullscreen game is in focus.

Detects fullscreen + borderless-windowed apps (window size == screen size).
A whitelist of "definitely not a game" processes prevents false positives
when, e.g., Brave is running YouTube fullscreen.

Hysteresis:
  - Foreground app detected as fullscreen for >120 sec  -> set paused = True
  - Foreground app NOT fullscreen for >30 sec           -> set paused = False

Scrapers call `is_paused()` before starting a cycle. In-flight scrapes finish
naturally — we don't kill anything.

Windows-only (uses pywin32). On other platforms `is_paused()` always returns
False, so the bot behaves normally.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)

# State, read by scrapers via is_paused()
_paused: bool = False
_pause_started_at: Optional[float] = None  # epoch when current pause began
_last_state_change: float = 0.0

# Hysteresis thresholds (seconds)
PAUSE_AFTER_FULLSCREEN_S = 120
RESUME_AFTER_NORMAL_S = 30
POLL_INTERVAL_S = 10

# Process names that look fullscreen but are NOT games
_WHITELIST = {
    "chrome.exe", "brave.exe", "firefox.exe", "msedge.exe", "opera.exe",
    "code.exe", "discord.exe", "explorer.exe",
    "obs64.exe", "obs.exe",  # streaming
    "powerpnt.exe", "winword.exe", "excel.exe",
    "vlc.exe", "mpc-hc64.exe", "mpc-hc.exe",
    # The bot itself shouldn't pause itself
    "python.exe", "pythonw.exe", "cmd.exe", "powershell.exe",
}


def is_paused() -> bool:
    """True iff scrapers should hold off starting new work."""
    return _paused


def get_state() -> dict:
    """Snapshot for /sys dashboard."""
    return {
        "paused": _paused,
        "pause_seconds": int(time.time() - _pause_started_at) if _paused and _pause_started_at else 0,
        "last_change_seconds_ago": int(time.time() - _last_state_change) if _last_state_change else None,
    }


def _foreground_process_name() -> str:
    """Return the .exe name (lowercased) of the foreground window's process."""
    try:
        import win32gui
        import win32process
        import psutil
        hwnd = win32gui.GetForegroundWindow()
        if not hwnd:
            return ""
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        if not pid:
            return ""
        return psutil.Process(pid).name().lower()
    except Exception:
        return ""


def _is_foreground_fullscreen() -> bool:
    """Window size == screen size on the same monitor (covers exclusive
    fullscreen and borderless-windowed). Returns False on non-Windows or any
    API failure, so the watcher fails safe (no pause)."""
    if sys.platform != "win32":
        return False
    try:
        import win32gui
        import win32api
        from ctypes import windll
        hwnd = win32gui.GetForegroundWindow()
        if not hwnd:
            return False
        # Skip the desktop / taskbar
        cls = win32gui.GetClassName(hwnd) or ""
        if cls in ("Progman", "WorkerW", "Shell_TrayWnd"):
            return False
        rect = win32gui.GetWindowRect(hwnd)
        win_w = rect[2] - rect[0]
        win_h = rect[3] - rect[1]
        # Find the monitor under the window for multi-display setups
        MONITOR_DEFAULTTONEAREST = 2
        hmon = windll.user32.MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST)
        # MONITORINFO struct: read via GetMonitorInfo
        from ctypes import Structure, c_ulong, c_long, byref, sizeof

        class RECT(Structure):
            _fields_ = [("left", c_long), ("top", c_long), ("right", c_long), ("bottom", c_long)]

        class MONITORINFO(Structure):
            _fields_ = [("cbSize", c_ulong), ("rcMonitor", RECT), ("rcWork", RECT), ("dwFlags", c_ulong)]

        mi = MONITORINFO()
        mi.cbSize = sizeof(MONITORINFO)
        if not windll.user32.GetMonitorInfoW(hmon, byref(mi)):
            # Fallback to primary screen metrics
            screen_w = win32api.GetSystemMetrics(0)
            screen_h = win32api.GetSystemMetrics(1)
        else:
            screen_w = mi.rcMonitor.right - mi.rcMonitor.left
            screen_h = mi.rcMonitor.bottom - mi.rcMonitor.top
        # Allow ~10 px tolerance for tiny Windows borders
        return win_w >= screen_w - 10 and win_h >= screen_h - 10
    except Exception:
        return False


def _should_treat_as_game() -> bool:
    """Foreground is fullscreen AND its process is not in the whitelist."""
    if not _is_foreground_fullscreen():
        return False
    proc = _foreground_process_name()
    if not proc:
        return False
    if proc in _WHITELIST:
        return False
    return True


async def watch_loop(notify_pause=None, notify_resume=None) -> None:
    """Run forever. Optionally call `notify_pause(proc_name)` /
    `notify_resume()` (async callables) on state transitions, e.g. to send
    a Telegram message.
    """
    global _paused, _pause_started_at, _last_state_change
    fullscreen_since: Optional[float] = None
    normal_since: Optional[float] = None

    while True:
        try:
            looks_like_game = _should_treat_as_game()
            now = time.time()

            if looks_like_game:
                normal_since = None
                if fullscreen_since is None:
                    fullscreen_since = now
                if not _paused and (now - fullscreen_since) >= PAUSE_AFTER_FULLSCREEN_S:
                    proc = _foreground_process_name()
                    _paused = True
                    _pause_started_at = now
                    _last_state_change = now
                    logger.info("[game-watcher] PAUSED — fullscreen process=%s", proc)
                    if notify_pause:
                        try:
                            await notify_pause(proc)
                        except Exception:
                            pass
            else:
                fullscreen_since = None
                if normal_since is None:
                    normal_since = now
                if _paused and (now - normal_since) >= RESUME_AFTER_NORMAL_S:
                    duration = now - (_pause_started_at or now)
                    _paused = False
                    _pause_started_at = None
                    _last_state_change = now
                    logger.info("[game-watcher] RESUMED — was paused %.0fs", duration)
                    if notify_resume:
                        try:
                            await notify_resume(duration)
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning("[game-watcher] tick failed: %s", exc)

        await asyncio.sleep(POLL_INTERVAL_S)
