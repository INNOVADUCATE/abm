@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================
REM CONFIG (PC A - OneDrive)
REM =========================
set "REPO=C:\Users\aguss\OneDrive\Documentos\GitHub\abm"
set "PIPE=%REPO%\pipeline"
set "RUNNER=%PIPE%\abm_run_pipeline.py"
set "MINIIN=%REPO%\runs\mini_in"
set "PROCDB=%REPO%\runs\mini_isolated_processed.sqlite"
set "RUNLOG=%REPO%\runs\mini_isolated_run.log"
set "JSON=%PIPE%\json_llm"

REM Helper temp python script for DB summary (avoids quoting hell)
set "DBPY=%REPO%\runs\db_summary_tmp.py"

:MENU
cls
echo =====================================================
echo ABM AGENT (CMD) - %date% %time%
echo REPO: %REPO%
echo =====================================================
echo 1) Git sanity (checkout/pull/status)
echo 2) Clean outputs (raw_ocr/json_llm/temp + db/log)
echo 3) Run MINI pipeline (runs\mini_in)
echo 4) Tail log (live)
echo 5) Status python.exe (cpu/mem)
echo 6) Kill python.exe (force)
echo 7) Report (counts + resumen + db summary)
echo 0) Exit
echo.

set /p OPT=Elegi opcion: 

if "%OPT%"=="1" goto GIT
if "%OPT%"=="2" goto CLEAN
if "%OPT%"=="3" goto RUN
if "%OPT%"=="4" goto TAIL
if "%OPT%"=="5" goto STATUS
if "%OPT%"=="6" goto KILL
if "%OPT%"=="7" goto REPORT
if "%OPT%"=="0" goto END
goto MENU

:GIT
echo === GIT ===
cd /d "%REPO%"
git checkout main
git pull
git status
pause
goto MENU

:CLEAN
echo === CLEAN ===
cd /d "%REPO%"
rmdir /s /q "%PIPE%\raw_ocr" 2>nul
rmdir /s /q "%PIPE%\json_llm" 2>nul
rmdir /s /q "%PIPE%\temp_proceso" 2>nul
del /q "%PROCDB%" 2>nul
del /q "%RUNLOG%" 2>nul
echo OK
pause
goto MENU

:RUN
echo === COUNT INPUT PDF ===
cd /d "%REPO%"
if not exist "%MINIIN%" (
  echo NO EXISTE: %MINIIN%
  pause
  goto MENU
)

for /f %%A in ('dir /s /b "%MINIIN%\*.pdf" 2^>nul ^| find /c /v ""') do set "PDFCOUNT=%%A"
echo mini_in pdfs: !PDFCOUNT!

echo.
echo === RUN PIPELINE (log: %RUNLOG%) ===
echo (Tip: usa opcion 4 para ver el log en vivo)
python "%RUNNER%" --ui-socios-dir "%MINIIN%" --dias-prox 30 --processed-db "%PROCDB%" --force-reprocess > "%RUNLOG%" 2>&1

echo.
echo === DONE ===
echo Abri el log si queres:
echo notepad "%RUNLOG%"
pause
goto MENU

:TAIL
echo === TAIL (CTRL+C para salir) ===
cd /d "%REPO%"
if not exist "%RUNLOG%" (
  echo (Aun no existe el log. Corriendo opcion 3 primero o creando archivo vacio...)
  type nul > "%RUNLOG%"
)
powershell -NoProfile -Command "Get-Content -Tail 60 -Wait '%RUNLOG%'"
pause
goto MENU

:STATUS
echo === python.exe status ===
cd /d "%REPO%"
tasklist /FI "IMAGENAME eq python.exe"
echo.
echo === (PowerShell) CPU/MEM/StartTime (si existe) ===
powershell -NoProfile -Command "Get-Process python -ErrorAction SilentlyContinue | Select-Object Id,CPU,WorkingSet,StartTime | Format-Table -Auto"
pause
goto MENU

:KILL
echo === KILL python.exe ===
taskkill /F /IM python.exe
pause
goto MENU

:REPORT
cd /d "%REPO%"
echo === COUNTS ===
for /f %%A in ('dir /s /b "%JSON%\03_aportes\socios\SOCIO_*.json" 2^>nul ^| find /c /v ""') do set "C03=%%A"
for /f %%A in ('dir /s /b "%JSON%\04_estado\socios\SOCIO_*_ESTADO.json" 2^>nul ^| find /c /v ""') do set "C04=%%A"
for /f %%A in ('dir /s /b "%JSON%\05_perfiles_socios\*.*" 2^>nul ^| find /c /v ""') do set "C05=%%A"
echo 03_socios: !C03!
echo 04_estado_socios: !C04!
echo 05_perfiles_socios files: !C05!

echo.
echo === resumen.json ===
if exist "%JSON%\04_estado\resumen.json" (
  type "%JSON%\04_estado\resumen.json"
) else (
  echo (no existe) Todavia no termino el pipeline o fallo antes de generar resumen.
)

echo.
echo === DB summary ===

REM write temp python script (safe quoting)
> "%DBPY%" echo import sqlite3
>>"%DBPY%" echo import os, sys
>>"%DBPY%" echo db = r"%PROCDB%"
>>"%DBPY%" echo if not os.path.exists(db):
>>"%DBPY%" echo ^    print("DB not found:", db); sys.exit(0)
>>"%DBPY%" echo con = sqlite3.connect(db)
>>"%DBPY%" echo cur = con.cursor()
>>"%DBPY%" echo print(cur.execute("select last_stage,status,count(*) from processed_files group by last_stage,status").fetchall())
>>"%DBPY%" echo print(cur.execute("select file_path,last_stage,status,coalesce(error_msg,'') from processed_files where status='error' limit 20").fetchall())
python "%DBPY%"
del /q "%DBPY%" 2>nul

echo.
echo === LOG last 30 ===
if exist "%RUNLOG%" (
  powershell -NoProfile -Command "Get-Content -Tail 30 '%RUNLOG%'"
) else (
  echo (no existe RUNLOG)
)
pause
goto MENU

:END
endlocal
exit /b 0