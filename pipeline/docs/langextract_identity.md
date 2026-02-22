# langextract_identity (fallback conservador)

Herramienta auxiliar para extraer identidad desde OCR TXT sin tocar el pipeline principal.

## Instalación opcional de LangExtract

```bash
pip install langextract
```

> Si `langextract` no está instalado, el script **no falla**: usa fallback por regex para `dni` / `cuil_cuit` y deja `display_name` vacío.

## Uso CLI

### Desde archivo OCR TXT

```bash
python CODIGO_AGUS/ABM/tools/langextract_identity.py \
  --input CODIGO_AGUS/ABM/json_llm/00_ocr/dni_OCR_LLM_READY.txt \
  --pretty
```

### Desde texto crudo

```bash
python CODIGO_AGUS/ABM/tools/langextract_identity.py \
  --text "DNI 12345678 CUIL 20-12345678-3" \
  --pretty
```

### Guardar salida en archivo

```bash
python CODIGO_AGUS/ABM/tools/langextract_identity.py \
  --input CODIGO_AGUS/ABM/json_llm/00_ocr/constancia_OCR_LLM_READY.txt \
  --out CODIGO_AGUS/ABM/temp_proceso/identity.json \
  --pretty
```

## Salida mínima

```json
{
  "display_name": "",
  "dni": "",
  "cuil_cuit": "",
  "apellido": "",
  "nombre": "",
  "confidence": 0.0,
  "evidence": {
    "spans": []
  }
}
```
