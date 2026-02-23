@echo off
:: ABM Pipeline Agent Launcher
:: This file does ONE thing only: launch the Python agent.
:: All logic lives in abm_agent.py and openclaw/

cd /d "%~dp0"
python abm_agent.py %*
