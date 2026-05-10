"""Pause the scrapers when a fullscreen game is in focus OR the user is
actively using the PC (mouse/keyboard input within the last 30 seconds).

Two independent triggers, OR'd together:
  1. Game pause — fullscreen + borderless-windowed apps (window size ==
     screen size). Prevents the bot from hogging CPU/GPU during gaming.
  2. User-active pause — keyboard or mouse input within USER_ACTIVE_S
     means "user is at the desk", so don't run heavy scrapes that
     could cause micro-lags. Sentinels (LBC/PAP) keep running, only the
     auto-loop cycles are gated.

Hysteresis:
  - Foreground app detected as fullscreen for >120 sec  -> game pause
  - Foreground app NOT fullscreen for >30 sec           -> game resume
  - Last input <30s ago                                  -> user-active pause
  - Last input >120s ago                                 -> user-active resume

Scrapers call `is_paused()` before starting a cycle. In-flight scrapes finish
naturally — we don't kill anything.

Windows-only (uses pywin32 + Win32 GetLastInputInfo). On other platforms
`is_paused()` always returns False, so the bot behaves normally.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)

# State, read by scrapers via is_paused()
_paused_game: bool = False  # fullscreen-game pause
_paused_user: bool = False  # user-active pause
_pause_started_at: Optional[float] = None  # epoch when current pause began
_last_state_change: float = 0.0

# Hysteresis thresholds (seconds)
PAUSE_AFTER_FULLSCREEN_S = 120
RESUME_AFTER_NORMAL_S = 30
USER_ACTIVE_S = 30      # last input within → pause heavy scrapes
USER_IDLE_S = 120       # last input older than → resume
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
    return _paused_game or _paused_user


def get_state() -> dict:
    """Snapshot for /sys dashboard."""
    paused = is_paused()
    return {
        "paused": paused,
        "paused_reason": (
            "game" if _paused_game else ("user-active" if _paused_user else None)
        ),
        "pause_seconds": int(time.time() - _pause_started_at) if paused and _pause_started_at else 0,
        "last_change_seconds_ago": int(time.time() - _last_state_change) if _last_state_change else None,
    }


def _user_idle_seconds() -> float:
    """Seconds since last keyboard/mouse input. Returns inf on non-Windows
    or any API failure (so the user-active pause silently disables)."""
    if sys.platform != "win32":
        return float("inf")
    try:
        import ctypes
        from ctypes import Structure, c_uint, byref, sizeof, windll

        class LASTINPUTINFO(Structure):
            _fields_ = [("cbSize", c_uint), ("dwTime", c_uint)]

        info = LASTINPUTINFO()
        info.cbSize = sizeof(LASTINPUTINFO)
        if not windll.user32.GetLastInputInfo(byref(info)):
            return float("inf")
        tick_now = windll.kernel32.GetTickCount()
        # GetTickCount wraps every ~49 days; clamp negative deltas to 0
        return max(0.0, (tick_now - info.dwTime) / 1000.0)
    except Exception:
        return float("inf")


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
    """Run forever. Optionally call `notify_pause(reason)` / `notify_resume()`
    (async callables) on aggregate-state transitions (only fires when
    is_paused() actually flips), e.g. to send a Telegram message.
    """
    global _paused_game, _paused_user, _pause_started_at, _last_state_change
    fullscreen_since: Optional[float] = None
    normal_since: Optional[float] = None

    while True:
        try:
            now = time.time()
            was_paused = _paused_game or _paused_user

            # ── Branch 1: fullscreen-game detection (existing logic) ──
            looks_like_game = _should_treat_as_game()
            if looks_like_game:
                normal_since = None
                if fullscreen_since is None:
                    fullscreen_since = now
                if not _paused_game and (now - fullscreen_since) >= PAUSE_AFTER_FULLSCREEN_S:
                    _paused_game = True
                    logger.info("[game-watcher] GAME pause — process=%s", _foreground_process_name())
            else:
                fullscreen_since = None
                if normal_since is None:
                    normal_since = now
                if _paused_game and (now - normal_since) >= RESUME_AFTER_NORMAL_S:
                    _paused_game = False
                    logger.info("[game-watcher] GAME resume")

            # ── Branch 2: user-active detection (new) ──
            idle_s = _user_idle_seconds()
            if idle_s < USER_ACTIVE_S:
                if not _paused_user:
                    _paused_user = True
                    logger.info("[game-watcher] USER-active pause (idle=%.0fs)", idle_s)
            elif idle_s >= USER_IDLE_S:
                if _paused_user:
                    _paused_user = False
                    logger.info("[game-watcher] USER-idle resume (idle=%.0fs)", idle_s)
            # else: 30 ≤ idle_s < 120 → keep current state (hysteresis band)

            # ── Aggregate transition + Telegram notification ──
            now_paused = _paused_game or _paused_user
            if now_paused != was_paused:
                _last_state_change = now
                if now_paused:
                    _pause_started_at = now
                    reason = "game" if _paused_game else "user-active"
                    if notify_pause:
                        try:
                            await notify_pause(reason)
                        except Exception:
                            pass
                else:
                    duration = now - (_pause_started_at or now)
                    _pause_started_at = None
                    if notify_resume:
                        try:
                            await notify_resume(duration)
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning("[game-watcher] tick failed: %s", exc)

        await asyncio.sleep(POLL_INTERVAL_S)
