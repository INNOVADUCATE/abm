@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "REPO=C:\Users\aguss\OneDrive\Documentos\GitHub\abm"
set "PIPE=%REPO%\pipeline"
set "RUNNER=%PIPE%\abm_run_pipeline.py"
set "MINIIN=%REPO%\runs\mini_in"

REM Logs/DB (alineado a lo que tenés en runs\)
set "PROCDB=%REPO%\runs\mini_isolated_processed.sqlite"
set "RUNLOG=%REPO%\runs\mini_full_run.log"
set "JSON=%PIPE%\json_llm"
set "PY=python"

:menu
cls
echo =========================================
for /f "tokens=1-4 delims=:.," %%a in ("%time%") do set "NOW=%%a:%%b:%%c"
echo ABM AGENT (CMD) - %date% %NOW%
echo REPO: %REPO%
echo =========================================
echo 1^) Git sanity (checkout/pull/status)
echo 2^) Clean outputs (raw_ocr/json_llm/temp + db/log)
echo 3^) Run MINI pipeline (runs\mini_in)  [BACKGROUND]
echo 4^) Tail log (live)                  [CTRL+C para salir]
echo 5^) Status python.exe (cpu/mem)
echo 6^) Kill python.exe (force)
echo 7^) Report (counts + resumen + db summary)
echo 0^) Exit
echo.
set /p opt=Elegí opción: 

if "%opt%"=="1" goto git
if "%opt%"=="2" goto clean
if "%opt%"=="3" goto runmini
if "%opt%"=="4" goto taillog
if "%opt%"=="5" goto statuspy
if "%opt%"=="6" goto killpy
if "%opt%"=="7" goto report
if "%opt%"=="0" goto end
goto menu

:git
echo === GIT ===
cd /d "%REPO%"
git checkout main
git pull
git status
pause
goto menu

:clean
echo === CLEAN ===
cd /d "%REPO%"
rmdir /s /q "%PIPE%\raw_ocr" 2>nul
rmdir /s /q "%PIPE%\json_llm" 2>nul
rmdir /s /q "%PIPE%\temp_proceso" 2>nul
del /q "%PROCDB%" 2>nul
del /q "%RUNLOG%" 2>nul
echo OK
pause
goto menu

:runmini
cd /d "%REPO%"

echo === COUNT INPUT PDF ===
for /f %%A in ('dir /s /b "%MINIIN%\*.pdf" 2^>nul ^| find /c /v ""') do set "PDFCOUNT=%%A"
echo mini_in pdfs: %PDFCOUNT%
echo.

echo === RUN PIPELINE (BACKGROUND) ===
echo Log: %RUNLOG%
echo (Tip: podés ir a opcion 4 para ver el log en vivo sin matar el proceso)
echo.

REM Start in background using PowerShell so we don't block this menu and we don't kill it with CTRL+C here
powershell -NoProfile -Command ^
  "$p=Start-Process -FilePath '%PY%' -ArgumentList '\"%RUNNER%\" --ui-socios-dir \"'%MINIIN%'\" --dias-prox 30 --processed-db \"'%PROCDB%'\" --force-reprocess' -RedirectStandardOutput \"'%RUNLOG%'\" -RedirectStandardError \"'%RUNLOG%'\" -NoNewWindow -PassThru; $p.Id | Out-File -Encoding ascii '%REPO%\runs\mini_pid.txt'; Write-Host ('Started PID=' + $p.Id)"

echo.
echo Guardé el PID en: runs\mini_pid.txt
pause
goto menu

:taillog
cd /d "%REPO%"
echo === TAIL (CTRL+C para salir) ===
if not exist "%RUNLOG%" (
  echo NO EXISTE: %RUNLOG%
  echo (corré opcion 3 primero)
  pause
  goto menu
)
powershell -NoProfile -Command "Get-Content -Tail 80 -Wait '%RUNLOG%'"
goto menu

:statuspy
echo === python.exe status ===
tasklist /FI "IMAGENAME eq python.exe"
echo.
echo (Si queres ver CPU/RAM mas claro:)
echo powershell -NoProfile -Command "Get-Process python ^| Sort CPU -Desc ^| Select -First 5 Id,CPU,WorkingSet,StartTime ^| Format-Table -Auto"
pause
goto menu

:killpy
echo === KILL python.exe ===
echo Si estás seguro:
echo taskkill /F /IM python.exe
echo.
taskkill /F /IM python.exe >nul 2>&1
echo OK
pause
goto menu

:report
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
  echo (no existe aun)
)
echo.

echo === DB summary ===
REM Create a small temp python file to avoid CMD quoting hell
set "TMPPY=%REPO%\runs\_db_summary_tmp.py"
> "%TMPPY%" (
  echo import sqlite3, sys
  echo db = r"%PROCDB%"
  echo con = sqlite3.connect(db)
  echo cur = con.cursor()
  echo print(cur.execute("select last_stage,status,count(*) from processed_files group by last_stage,status").fetchall())
  echo print(cur.execute("select file_path,last_stage,status,coalesce(error_msg,'') from processed_files where status='error' limit 20").fetchall())
)
%PY% "%TMPPY%"
del /q "%TMPPY%" 2>nul

echo.
echo === LOG last 30 ===
if exist "%RUNLOG%" (
  powershell -NoProfile -Command "Get-Content -Tail 30 '%RUNLOG%'"
) else (
  echo (no hay log aun)
)
pause
goto menu

:end
endlocal
exit /b 0