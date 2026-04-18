import os, logging, requests, csv
from datetime import datetime, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright
import gspread
from google.oauth2.service_account import Credentials
from config import LIVERPOOL, GOOGLE, CHAT, CARPETA_DESCARGA

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(f"logs/{datetime.now():%Y-%m-%d}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

WEBHOOK = "https://chat.googleapis.com/v1/spaces/AAQAQ6DrmfI/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VzOYmkn9w65FPf64JLq1ySI0VFyD5E8sdc-KQc29nXw"

def get_fechas():
    hoy  = datetime.now()
    ayer = hoy - timedelta(days=1)
    return ayer.strftime("%d/%m/%Y"), hoy.strftime("%d/%m/%Y")

def convertir_valor(v):
    """Convierte strings a número si es posible para evitar comilla en Sheets."""
    if v is None or v == '':
        return ''
    try:
        if '.' not in str(v):
            return int(v)
        else:
            return float(v)
    except (ValueError, TypeError):
        return str(v)

def descargar_csv() -> str:
    Path(CARPETA_DESCARGA).mkdir(parents=True, exist_ok=True)
    fecha_ayer, fecha_hoy = get_fechas()
    log.info(f"Descargando reporte del {fecha_ayer} al {fecha_hoy}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            accept_downloads=True
        )
        page = context.new_page()
        page.goto(LIVERPOOL["url_login"], timeout=60000)
        page.wait_for_load_state("domcontentloaded")
        log.info("Página cargada, iniciando login...")
        import time
        time.sleep(4)

        page.mouse.click(683, 272)
        time.sleep(1)
        page.keyboard.type(LIVERPOOL["usuario"], delay=80)
        page.mouse.click(683, 344)
        time.sleep(1)
        page.keyboard.type(LIVERPOOL["password"], delay=80)
        page.mouse.click(683, 480)
        log.info("Login enviado...")
        time.sleep(6)

        page.mouse.click(683, 114)
        log.info("Clic en INDICADORES...")
        time.sleep(5)

        page.mouse.click(1275, 100)
        log.info("Calendario abierto...")
        time.sleep(2)

        coords = {
            1:  (630, 240), 2:  (683, 240), 3:  (735, 240),
            4:  (788, 240), 5:  (840, 240),
            6:  (524, 290), 7:  (577, 290), 8:  (630, 290),
            9:  (683, 290), 10: (735, 290), 11: (788, 290), 12: (840, 290),
            13: (524, 340), 14: (577, 340), 15: (630, 340),
            16: (683, 340), 17: (735, 340), 18: (788, 340), 19: (840, 340),
            20: (524, 390), 21: (577, 390), 22: (630, 390),
            23: (683, 390), 24: (735, 390), 25: (788, 390), 26: (840, 390),
            27: (524, 440), 28: (577, 440), 29: (630, 440), 30: (683, 440), 31: (735, 440),
        }

        hoy_dia  = datetime.now().day
        ayer_dia = (datetime.now() - timedelta(days=1)).day

        ax, ay = coords[ayer_dia]
        page.mouse.click(ax, ay)
        log.info(f"Día {ayer_dia} seleccionado...")
        time.sleep(1)

        hx, hy = coords[hoy_dia]
        page.mouse.click(hx, hy)
        log.info(f"Día {hoy_dia} seleccionado...")
        time.sleep(1)

        page.mouse.click(1321, 24)
        log.info("Periodo guardado, esperando datos...")
        time.sleep(30)

        log.info("Descargando archivo...")
        with page.expect_download(timeout=180000) as dl_info:
            page.mouse.click(1275, 100)
            time.sleep(1)

        download = dl_info.value
        nombre   = download.suggested_filename
        destino  = os.path.join(CARPETA_DESCARGA, nombre)
        download.save_as(destino)
        browser.close()

    log.info(f"Archivo guardado: {destino} ✅")
    return destino

def leer_csv(ruta: str) -> tuple:
    datos = []
    with open(ruta, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            fila_convertida = [convertir_valor(v) for v in row]
            datos.append(fila_convertida)

    col_status  = 8
    espera      = sum(1 for r in datos if len(r) > col_status and r[col_status] == "Mercancia en Espera de Entrega")
    etiquetas   = sum(1 for r in datos if len(r) > col_status and r[col_status] == "Etiqueta Generada")
    sin_asignar = sum(1 for r in datos if len(r) > col_status and r[col_status] == "Sin Asignar")
    rechazados  = sum(1 for r in datos if len(r) > col_status and r[col_status] == "Rechazado")

    resumen = {
        "total":       len(datos),
        "espera":      espera,
        "etiquetas":   etiquetas,
        "sin_asignar": sin_asignar,
        "rechazados":  rechazados,
    }
    log.info(f"CSV leído: {len(datos)} filas ✅")
    return datos, resumen

def aplicar_formato(ss, hoja_app, num_filas):
    sheet_id = hoja_app.id
    try:
        meta = ss.fetch_sheet_metadata()
        limpiar = []
        for s in meta['sheets']:
            if s['properties']['sheetId'] == sheet_id:
                for b in s.get('bandedRanges', []):
                    limpiar.append({"deleteBanding": {"bandedRangeId": b['bandedRangeId']}})
                for i in range(len(s.get('conditionalFormats', []))):
                    limpiar.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}})
        if limpiar:
            ss.batch_update({"requests": limpiar})
    except Exception:
        pass

    ss.batch_update({"requests": [
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
            "fields": "gridProperties.frozenRowCount"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 9},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10}
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 9},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.914, "green": 0.118, "blue": 0.549},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 10},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
        }},
        {"addBanding": {"bandedRange": {
            "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": num_filas + 3, "startColumnIndex": 0, "endColumnIndex": 9},
            "rowProperties": {
                "firstBandColor":  {"red": 1,    "green": 1,    "blue": 1},
                "secondBandColor": {"red": 0.97, "green": 0.90, "blue": 0.96}
            }
        }}},
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": num_filas + 3, "startColumnIndex": 0, "endColumnIndex": 9}],
            "booleanRule": {
                "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "Mercancia en Espera"}]},
                "format": {"backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.6}}
            }
        }, "index": 0}},
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": num_filas + 3, "startColumnIndex": 0, "endColumnIndex": 9}],
            "booleanRule": {
                "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "Etiqueta Generada"}]},
                "format": {"backgroundColor": {"red": 0.72, "green": 0.93, "blue": 0.72}}
            }
        }, "index": 1}},
        {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 9}}},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": num_filas + 3},
            "properties": {"pixelSize": 22},
            "fields": "pixelSize"
        }},
    ]})
    log.info("Formato APP 2.0 aplicado ✅")

def actualizar_sheets(gc, datos: list) -> None:
    ss1 = gc.open_by_key(GOOGLE["sheet_id"])

    # ── Sheet 1 — reporte completo sin comillas ───────────────
    try:
        hoja1 = ss1.worksheet(GOOGLE["nombre_hoja"])
    except gspread.WorksheetNotFound:
        hoja1 = ss1.add_worksheet(GOOGLE["nombre_hoja"], rows=5000, cols=50)
    hoja1.clear()
    hoja1.update(datos, "A1", value_input_option="RAW")
    log.info("Sheet 1 actualizado ✅")

    # ── Sheet 2 — datos + timestamp en AS2 ───────────────────
    ss2 = gc.open_by_key(GOOGLE["sheet2_id"])
    try:
        hoja2 = ss2.worksheet(GOOGLE["sheet2_hoja"])
    except gspread.WorksheetNotFound:
        hoja2 = ss2.add_worksheet(GOOGLE["sheet2_hoja"], rows=5000, cols=50)

    hoja2.update(datos, f"{GOOGLE['sheet2_col']}{GOOGLE['sheet2_fila']}", value_input_option="RAW")

    # Timestamp en celda AS2
    fecha_actualizacion = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    celda_timestamp = f"{GOOGLE['timestamp_col']}{GOOGLE['timestamp_fila']}"
    hoja2.update([[fecha_actualizacion]], celda_timestamp)
    log.info(f"Sheet 2 actualizado ✅ | Timestamp {celda_timestamp}: {fecha_actualizacion}")

    # ── APP 2.0 ───────────────────────────────────────────────
    try:
        hoja_app = ss1.worksheet('APP 2.0')
    except gspread.WorksheetNotFound:
        hoja_app = ss1.add_worksheet('APP 2.0', rows=5000, cols=15)

    hoja_app.clear()

    try:
        hoja_dir  = ss1.worksheet('DIRECTORIO')
        datos_dir = hoja_dir.get_all_values()
        dir_dict  = {}
        for row in datos_dir[1:]:
            if row and row[0]:
                sec = str(row[0]).strip()
                dir_dict[sec] = {
                    'jefe':      row[2] if len(row) > 2 else '',
                    'ubicacion': row[5] if len(row) > 5 else '',
                }
    except Exception:
        dir_dict = {}

    ubicaciones = sorted(set([v['ubicacion'] for v in dir_dict.values() if v['ubicacion']]))
    opciones    = ["Todas"] + ubicaciones

    hoja_app.update([["🔍 FILTRAR POR UBICACIÓN →", "", "", "Todas", "", "← Haz clic en D1 y selecciona una ubicación"]], "A1")

    ss1.batch_update({"requests": [{"setDataValidation": {
        "range": {"sheetId": hoja_app.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 3, "endColumnIndex": 4},
        "rule": {
            "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": op} for op in opciones]},
            "showCustomUi": True,
            "strict": False,
        }
    }}]})

    hoja_app.update([["REMISION","SKU","DESCRIPCION","CANTIDAD","COLABORADOR","SECCION","JEFE","UBICACIÓN","ESTATUS"]], "A2")

    COL_REMISION=1; COL_SKU=2; COL_DESCRIPCION=3; COL_CANTIDAD=4
    COL_SECCION=5;  COL_STATUS=8; COL_COLABORADOR=13; COL_JEFE=17

    ESTATUS_FILTRO = ["Etiqueta Generada", "Mercancia en Espera de Entrega"]

    rows_app = []
    for row in datos:
        if not row or len(row) <= COL_COLABORADOR:
            continue
        status = str(row[COL_STATUS]).strip() if len(row) > COL_STATUS else ''
        if status not in ESTATUS_FILTRO:
            continue
        sec       = str(row[COL_SECCION]).strip().replace('.0','') if len(row) > COL_SECCION else ''
        jefe      = str(row[COL_JEFE]).strip() if len(row) > COL_JEFE and row[COL_JEFE] else ''
        ubicacion = dir_dict.get(sec, {}).get('ubicacion', '')
        if not jefe or jefe in ('','nan','Sin Asignar','UNASSIGNED'):
            jefe = dir_dict.get(sec, {}).get('jefe', 'Sin Asignar')
        rows_app.append([
            row[COL_REMISION] if len(row)>COL_REMISION else '',
            row[COL_SKU] if len(row)>COL_SKU else '',
            row[COL_DESCRIPCION] if len(row)>COL_DESCRIPCION else '',
            row[COL_CANTIDAD] if len(row)>COL_CANTIDAD else '',
            row[COL_COLABORADOR] if len(row)>COL_COLABORADOR else '',
            sec, jefe, ubicacion, status,
        ])

    if rows_app:
        hoja_app.update(rows_app, "A3", value_input_option="RAW")

    try:
        aplicar_formato(ss1, hoja_app, len(rows_app))
    except Exception as e:
        log.error(f"Error aplicando formato: {e}")

    log.info(f"APP 2.0 actualizada: {len(rows_app)} filas ✅")

    try:
        from actualizar_directorio import actualizar_directorio_e_historial
        actualizar_directorio_e_historial(gc, GOOGLE["sheet_id"])
    except Exception as e:
        log.error(f"Error actualizando directorio: {e}")

def enviar_chat(resumen: dict, exito: bool = True, error: str = "") -> None:
    fecha_ayer, fecha_hoy = get_fechas()
    fecha_now = datetime.now().strftime("%d/%m/%Y %H:%M")
    if exito:
        texto = (
            f"📊 *Indicadores Liverpool 456*\n"
            f"🕐 Actualización: {fecha_now}\n"
            f"📅 Periodo: {fecha_ayer} → {fecha_hoy}\n\n"
            f"⏳ Mercancía en Espera: *{resumen.get('espera', 0)}*\n"
            f"🏷️ Etiquetas Generadas: *{resumen.get('etiquetas', 0)}*\n"
            f"⚠️ Sin Asignar: *{resumen.get('sin_asignar', 0)}*\n"
            f"❌ Rechazados: *{resumen.get('rechazados', 0)}*\n"
            f"📋 Total Remisiones: *{resumen.get('total', 0)}*\n\n"
            f"✅ Actualización completada exitosamente"
        )
    else:
        texto = f"⚠️ *Indicadores Liverpool 456* — Error ({fecha_now})\n{error}"
    requests.post(WEBHOOK, json={"text": texto})
    log.info("Mensaje Chat enviado ✅")

def main():
    log.info("=" * 50)
    log.info("Iniciando automatización Liverpool")
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(GOOGLE["credentials"], scopes=scopes)
        gc    = gspread.authorize(creds)
        ruta           = descargar_csv()
        datos, resumen = leer_csv(ruta)
        actualizar_sheets(gc, datos)
        enviar_chat(resumen, exito=True)
        log.info("✅ Proceso completado con éxito")
    except Exception as e:
        log.error(f"❌ Error: {e}")
        enviar_chat({}, exito=False, error=str(e))

if __name__ == "__main__":
    main()
