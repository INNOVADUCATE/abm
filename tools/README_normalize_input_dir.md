# normalize_input_dir.py

Utilidad para preparar una carpeta con PDFs sueltos y convertirla en estructura por socio compatible con el pipeline/UI.

## Uso

```bash
python normalize_input_dir.py --in "<dir_pool>" --out "<dir_normalizado>"
```

Con OCR opcional para casos SIN_ID:

```bash
python normalize_input_dir.py --in "<pool>" --out "<out>" --scan-pdf-text --ocr
```

Fallback por nombre de archivo (cuando no hay ID en filename/texto/OCR):

```bash
python normalize_input_dir.py --in "<pool>" --out "<out>" --group-by-filename-name
```

## Qué hace

1. Detecta por **filename**: primero **CUIL/CUIT (11 dígitos)**, luego **DNI (7-8 dígitos)**.
2. Si no encuentra y activás `--scan-pdf-text`, busca en texto embebido del PDF (sin OCR).
3. Si sigue sin ID y activás `--ocr`, ejecuta OCR mínimo:
   - sólo para PDFs que quedaron SIN_ID,
   - sólo en páginas candidatas (por defecto `[0, última]`),
   - prueba rotaciones `0/90/180/270` y elige mejor score.
4. Copia (no mueve) PDFs a:
   - `CUIL_<id>/...`
   - `DNI_<id>/...`
   - `SIN_ID/<hash>__<filename>.pdf`
5. Genera CSV `index.csv` con trazabilidad completa.

## CSV de salida

Columnas:
`filename, detected_id_type, detected_id, dest_path, method, errors, candidate_pages, rotation_used, ocr_used, confidence, notes`

## Opciones útiles

- `--scan-pdf-text`: activa detección en texto embebido.
- `--ocr`: activa OCR liviano sólo para SIN_ID.
- `--max-ocr-pages 2`: máximo de páginas por PDF para OCR.
- `--ocr-topk 0`: páginas extra por score simple (densidad de dígitos + keywords), además de `[0,last]`.
- `--csv "<ruta.csv>"`: cambia la ruta del CSV.
- `--group-by-filename-name`: cuando no se detecta DNI/CUIL, intenta agrupar por nombre derivado del filename (`NOMBRE_<SOCIO>`).

## Dependencias

- Python 3.10+
- `pypdf` para `--scan-pdf-text`
- `PyMuPDF` (`fitz`) + `paddleocr` para `--ocr`

```bash
pip install pypdf pymupdf paddleocr
```
