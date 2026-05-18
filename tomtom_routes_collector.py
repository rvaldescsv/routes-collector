"""
tomtom_routes_collector.py
--------------------------
Recolector de datos de rutas desde la API de TomTom Routing.
Lee las rutas desde un archivo JSON (routes.json) en la misma carpeta.

Estructura esperada del JSON:
{
  "routes": [
    {
      "id": 1,
      "category": "short" | "mid" | "long",
      "distance_km": 2.286,
      "origin_group": "A",
      "dest_group": "B",
      "origin_lat": -23.628474,
      "origin_lon": -70.389388,
      "dest_lat": -23.649031,
      "dest_lon": -70.388931
    },
    ...
  ]
}

Variables de entorno en Railway:
  TOMTOM_API_KEY   → clave de la API de TomTom
  DATABASE_URL     → se inyecta automáticamente con el plugin Postgres

Dependencias:
  pip install requests pandas sqlalchemy psycopg2-binary python-dotenv
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────

load_dotenv()

API_KEY      = os.getenv("TOMTOM_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL     = "https://api.tomtom.com/routing/1/calculateRoute"
TABLE_NAME   = "tomtom_routes"
TIMEZONE     = ZoneInfo("America/Santiago")

# Rango horario operacional (igual que el colector de flujo)
HOUR_START = 7   # 07:00
HOUR_END   = 22  # 22:00 (última consulta a las 21:xx)

# Pausa entre llamadas a la API para no saturar
SLEEP_BETWEEN_CALLS = 0.5  # segundos

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Carga de rutas desde JSON
# ─────────────────────────────────────────────

def load_routes(json_path: str = "routes.json") -> list[dict]:
    """
    Lee el archivo routes.json desde la misma carpeta que el script.
    Lanza un error claro si el archivo no existe o tiene formato incorrecto.
    """
    path = Path(__file__).parent / json_path

    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró '{json_path}' en {path.parent}. "
            "Asegúrate de subir el archivo JSON junto al script."
        )

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if "routes" not in data:
        raise ValueError("El JSON debe tener una clave 'routes' con la lista de rutas.")

    routes = data["routes"]
    required_keys = {"id", "origin_lat", "origin_lon", "dest_lat", "dest_lon"}

    for i, route in enumerate(routes):
        missing = required_keys - set(route.keys())
        if missing:
            raise ValueError(f"Ruta índice {i} le faltan campos: {missing}")

    log.info(
        "Rutas cargadas: %d total | short=%d mid=%d long=%d",
        len(routes),
        sum(1 for r in routes if r.get("category") == "short"),
        sum(1 for r in routes if r.get("category") == "mid"),
        sum(1 for r in routes if r.get("category") == "long"),
    )
    return routes


# ─────────────────────────────────────────────
# Conexión a Postgres
# ─────────────────────────────────────────────

def get_engine():
    if not DATABASE_URL:
        raise EnvironmentError(
            "DATABASE_URL no está definida. "
            "Agrega el plugin Postgres en Railway."
        )
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True)


def ensure_table_exists(engine) -> None:
    """
    Crea la tabla si no existe. Idempotente — seguro de relanzar.
    La clave única (route_id, requested_depart_at) evita duplicados
    si el cron se ejecuta más de una vez en el mismo ciclo horario.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id                          SERIAL PRIMARY KEY,

        -- Identificación de la ruta
        route_id                    INTEGER,
        category                    TEXT,
        distance_km_nominal         DOUBLE PRECISION,
        origin_group                TEXT,
        dest_group                  TEXT,

        -- Coordenadas
        origin_lat                  DOUBLE PRECISION,
        origin_lon                  DOUBLE PRECISION,
        dest_lat                    DOUBLE PRECISION,
        dest_lon                    DOUBLE PRECISION,

        -- Parámetros de consulta
        requested_depart_at         TIMESTAMPTZ,
        travel_mode                 TEXT,
        route_type                  TEXT,

        -- TARGET del modelo
        travel_time_s               INTEGER,

        -- FEATURES temporales
        hour_of_day                 SMALLINT,
        day_of_week                 SMALLINT,
        is_weekend                  SMALLINT,
        month                       SMALLINT,

        -- FEATURES de tráfico (seguros para el modelo)
        length_m                    INTEGER,
        no_traffic_time_s           INTEGER,
        historic_time_s             INTEGER,
        congestion_ratio            DOUBLE PRECISION,

        -- Informativos (no usar como features: data leakage)
        traffic_delay_s             INTEGER,
        live_traffic_time_s         INTEGER,
        historic_vs_live_delta_s    INTEGER,

        -- Tiempos devueltos por la API
        api_departure_time          TIMESTAMPTZ,
        api_arrival_time            TIMESTAMPTZ,

        collected_at                TIMESTAMPTZ DEFAULT NOW(),

        -- Evitar duplicados exactos
        UNIQUE (route_id, requested_depart_at)
    );
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    log.info("Tabla '%s' lista.", TABLE_NAME)


# ─────────────────────────────────────────────
# Llamada a la API
# ─────────────────────────────────────────────

def call_calculate_route(
    origin: tuple[float, float],
    destination: tuple[float, float],
    depart_at: datetime,
    travel_mode: str = "car",
    route_type: str = "fastest",
) -> dict | None:
    """
    Llama al endpoint calculateRoute de TomTom.
    Retorna el JSON completo o None si hay error.
    """
    origin_str = f"{origin[0]},{origin[1]}"
    dest_str   = f"{destination[0]},{destination[1]}"
    locations  = f"{origin_str}:{dest_str}"
    depart_str = depart_at.strftime("%Y-%m-%dT%H:%M:%S")

    params = {
        "key":                  API_KEY,
        "travelMode":           travel_mode,
        "routeType":            route_type,
        "traffic":              "true",
        "departAt":             depart_str,
        "computeTravelTimeFor": "all",
        "sectionType":          "traffic",
        "report":               "effectiveSettings",
    }

    url = f"{BASE_URL}/{locations}/json"

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        log.warning("HTTP %s | ruta %s → %s", e.response.status_code, origin_str, dest_str)
    except requests.exceptions.Timeout:
        log.warning("Timeout | ruta %s → %s", origin_str, dest_str)
    except requests.exceptions.RequestException as e:
        log.error("Error de red: %s", e)
    return None


# ─────────────────────────────────────────────
# Parsear respuesta
# ─────────────────────────────────────────────

def parse_response(
    response: dict,
    route: dict,
    requested_depart_at: datetime,
    travel_mode: str,
    route_type: str,
) -> dict | None:
    """
    Extrae los campos relevantes de la respuesta de TomTom
    y los combina con los metadatos de la ruta del JSON.
    """
    try:
        summary = response["routes"][0]["summary"]

        travel_time_s   = summary["travelTimeInSeconds"]
        traffic_delay_s = summary.get("trafficDelayInSeconds", 0)
        length_m        = summary["lengthInMeters"]
        no_traffic_s    = summary.get("noTrafficTravelTimeInSeconds")
        historic_s      = summary.get("historicTrafficTravelTimeInSeconds")
        live_s          = summary.get("liveTrafficIncidentsTravelTimeInSeconds")

        dep_str = summary.get("departureTime", "")
        arr_str = summary.get("arrivalTime", "")
        dep_time = datetime.fromisoformat(dep_str) if dep_str else None
        arr_time = datetime.fromisoformat(arr_str) if arr_str else None

        congestion_ratio = (
            round(travel_time_s / no_traffic_s, 4)
            if no_traffic_s and no_traffic_s > 0 else None
        )
        historic_vs_live = (
            travel_time_s - historic_s if historic_s is not None else None
        )

        return {
            # Identificación de la ruta (desde el JSON)
            "route_id":               route["id"],
            "category":               route.get("category"),
            "distance_km_nominal":    route.get("distance_km"),
            "origin_group":           route.get("origin_group"),
            "dest_group":             route.get("dest_group"),

            # Coordenadas
            "origin_lat":             route["origin_lat"],
            "origin_lon":             route["origin_lon"],
            "dest_lat":               route["dest_lat"],
            "dest_lon":               route["dest_lon"],

            # Parámetros de consulta
            "requested_depart_at":    requested_depart_at,
            "travel_mode":            travel_mode,
            "route_type":             route_type,

            # TARGET
            "travel_time_s":          travel_time_s,

            # Features temporales
            "hour_of_day":            requested_depart_at.hour,
            "day_of_week":            requested_depart_at.weekday(),
            "is_weekend":             int(requested_depart_at.weekday() >= 5),
            "month":                  requested_depart_at.month,

            # Features de tráfico
            "length_m":               length_m,
            "no_traffic_time_s":      no_traffic_s,
            "historic_time_s":        historic_s,
            "congestion_ratio":       congestion_ratio,

            # Informativos
            "traffic_delay_s":        traffic_delay_s,
            "live_traffic_time_s":    live_s,
            "historic_vs_live_delta_s": historic_vs_live,

            # Tiempos API
            "api_departure_time":     dep_time,
            "api_arrival_time":       arr_time,

            "collected_at":           datetime.now(TIMEZONE),
        }

    except (KeyError, IndexError, TypeError) as e:
        log.warning("Error al parsear ruta id=%s: %s", route.get("id"), e)
        return None


# ─────────────────────────────────────────────
# Guardar en Postgres con upsert
# ─────────────────────────────────────────────

def upsert_records(engine, records: list[dict]) -> None:
    """
    Inserta registros nuevos. Si ya existe (route_id, requested_depart_at),
    actualiza los valores. Seguro de relanzar sin duplicar.
    """
    if not records:
        log.info("Sin registros para guardar en este ciclo.")
        return

    df = pd.DataFrame(records)
    cols         = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    update_cols  = [c for c in df.columns if c not in ("route_id", "requested_depart_at")]
    updates      = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = text(f"""
        INSERT INTO {TABLE_NAME} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (route_id, requested_depart_at)
        DO UPDATE SET {updates}
    """)

    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))

    log.info("Upsert completado: %d registros.", len(records))


# ─────────────────────────────────────────────
# Exportar a Parquet (uso local para entrenar)
# ─────────────────────────────────────────────

def export_to_parquet(engine, output_path: str = "tomtom_routes.parquet") -> None:
    """
    Exporta toda la tabla a Parquet para usar en el entrenamiento del modelo.
    Ejecutar localmente:
        railway run python -c "
        from tomtom_routes_collector import get_engine, export_to_parquet
        export_to_parquet(get_engine())
        "
    """
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY collected_at", engine)
    df.to_parquet(output_path, index=False)
    log.info("Exportado %d filas a '%s'.", len(df), output_path)
    return df


# ─────────────────────────────────────────────
# Entrypoint — cron job horario
# ─────────────────────────────────────────────

if __name__ == "__main__":

    # Validaciones iniciales
    if not API_KEY:
        raise EnvironmentError("TOMTOM_API_KEY no está definida en las variables de entorno.")

    now = datetime.now(TIMEZONE)

    # Respetar rango horario operacional
    if not (HOUR_START <= now.hour < HOUR_END):
        log.info(
            "Fuera del rango horario (%02d:00–%02d:00). "
            "Hora actual: %s. Sin acción.",
            HOUR_START, HOUR_END, now.strftime("%H:%M")
        )
        raise SystemExit(0)

    # Normalizar al inicio de la hora actual
    depart_at = now.replace(minute=0, second=0, microsecond=0)

    log.info("=== Ciclo de recolección: %s ===", depart_at.strftime("%Y-%m-%d %H:%M %Z"))

    # Cargar rutas desde JSON
    routes = load_routes("routes.json")

    # Conectar a Postgres
    engine = get_engine()
    ensure_table_exists(engine)

    # Consultar cada ruta
    records = []
    errors  = 0

    for route in routes:
        response = call_calculate_route(
            origin      = (route["origin_lat"], route["origin_lon"]),
            destination = (route["dest_lat"],   route["dest_lon"]),
            depart_at   = depart_at,
        )

        if response is None:
            errors += 1
            continue

        record = parse_response(
            response            = response,
            route               = route,
            requested_depart_at = depart_at,
            travel_mode         = "car",
            route_type          = "fastest",
        )

        if record:
            records.append(record)

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Guardar en Postgres
    upsert_records(engine, records)

    log.info(
        "Ciclo finalizado. Guardados: %d | Errores: %d | Total rutas: %d",
        len(records), errors, len(routes)
    )