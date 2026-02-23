@echo off
setlocal

set "REPO=C:\Users\aguss\OneDrive\Documentos\GitHub\abm"
cd /d "%REPO%"

echo === GIT ===
git checkout main
git pull
git status

echo === PATHS ===
set "PIPE=%REPO%\pipeline"
set "RUNNER=%PIPE%\abm_run_pipeline.py"
set "MINIIN=%REPO%\runs\mini_in"
set "PROCDB=%REPO%\runs\mini_isolated_processed.sqlite"
set "RUNLOG=%REPO%\runs\mini_isolated_run.log"
set "JSON=%PIPE%\json_llm"

echo === CLEAN ===
rmdir /s /q "%PIPE%\raw_ocr" 2>nul
rmdir /s /q "%PIPE%\json_llm" 2>nul
rmdir /s /q "%PIPE%\temp_proceso" 2>nul
del /q "%PROCDB%" 2>nul
del /q "%RUNLOG%" 2>nul

echo === COUNT INPUT PDF ===
for /f %%A in ('dir /s /b "%MINIIN%\*.pdf" 2^>nul ^| find /c /v ""') do set "PDFCOUNT=%%A"
echo mini_in pdfs: %PDFCOUNT%

echo === RUN PIPELINE ===
python "%RUNNER%" --ui-socios-dir "%MINIIN%" --dias-prox 30 --processed-db "%PROCDB%" --force-reprocess > "%RUNLOG%" 2>&1

echo === LOG TAIL (last 60) ===
powershell -NoProfile -Command "Get-Content -Tail 60 '%RUNLOG%'"

echo === COUNTS ===
for /f %%A in ('dir /s /b "%JSON%\03_aportes\socios\SOCIO_*.json" 2^>nul ^| find /c /v ""') do set "C03=%%A"
for /f %%A in ('dir /s /b "%JSON%\04_estado\socios\SOCIO_*_ESTADO.json" 2^>nul ^| find /c /v ""') do set "C04=%%A"
for /f %%A in ('dir /s /b "%JSON%\05_perfiles_socios\*.*" 2^>nul ^| find /c /v ""') do set "C05=%%A"
echo 03_socios: %C03%
echo 04_estado_socios: %C04%
echo 05_perfiles_socios files: %C05%

echo === resumen.json ===
type "%JSON%\04_estado\resumen.json"

echo === DB summary ===
python -c "import sqlite3; con=sqlite3.connect(r'%PROCDB%'); cur=con.cursor(); print(cur.execute('select last_stage,status,count(*) from processed_files group by last_stage,status').fetchall()); print(cur.execute('select file_path,last_stage,status,coalesce(error_msg,'''') from processed_files where status=''error'' limit 20').fetchall())"

endlocal
^Z
.\run_mini.cmd
