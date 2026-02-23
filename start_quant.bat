@echo off
title Quant Engine Auto-Starter
echo ===================================================
echo   Starting Quant Trading Architecture...
echo ===================================================
echo.

REM Force the terminal to recognize the "Quant Aspirations" folder path
cd /d "%~dp0"

echo Booting Python FastAPI Backend...
start cmd /k "cd backend && py -m uvicorn main:app --host 0.0.0.0 --port 8000"

echo Booting Next.js Frontend...
start cmd /k "cd frontend && npm run dev -- -H 0.0.0.0"

echo.
echo ===================================================
echo   ✅ Servers are booting up in separate windows!
echo   You can now access your dashboard via Tailscale.
echo ===================================================
pause