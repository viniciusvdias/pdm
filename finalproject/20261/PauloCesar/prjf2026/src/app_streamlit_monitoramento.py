import os
from statistics import mean

import psycopg2
from psycopg2.extras import RealDictCursor
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "incendios_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin_password")

DEFAULT_REFRESH_SECONDS = 60
DEFAULT_LIMIT = 30

COLOR_SEM_RISCO = [30, 136, 229, 180]
COLOR_PROVAVEL = [255, 193, 7, 220]
COLOR_IMINENTE = [220, 53, 69, 240]


def schedule_refresh(refresh_seconds: int) -> None:
    components.html(
        f"""
        <script>
            window.setTimeout(function() {{
                window.parent.location.reload();
            }}, {refresh_seconds * 1000});
        </script>
        """,
        height=0,
    )


def fetch_latest_sensor_data(limit: int) -> list[dict]:
    query = """
        SELECT *
        FROM (
            SELECT DISTINCT ON (sensor_id)
                sensor_id,
                timestamp,
                latitude,
                longitude,
                temperatura,
                umidade,
                co2,
                status_ia_borda,
                indice_risco
            FROM historico_sensores
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY sensor_id, timestamp DESC
        ) sensores
        ORDER BY timestamp DESC
        LIMIT %s;
    """

    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            return list(cursor.fetchall())


def classify_risk(sensor: dict, probable_threshold: float, imminent_threshold: float) -> tuple[str, list[int]]:
    iri = float(sensor.get("indice_risco") or 0.0)
    edge_status = sensor.get("status_ia_borda") or "NORMAL"

    if edge_status == "ALERTA_INCENDIO" or iri >= imminent_threshold:
        return "Risco Iminente", COLOR_IMINENTE
    if iri >= probable_threshold:
        return "Provavel Risco", COLOR_PROVAVEL
    return "Sem Risco", COLOR_SEM_RISCO


def enrich_sensor_data(
    sensors: list[dict], probable_threshold: float, imminent_threshold: float
) -> list[dict]:
    enriched = []
    for sensor in sensors:
        risk_label, color = classify_risk(sensor, probable_threshold, imminent_threshold)
        enriched.append(
            {
                **sensor,
                "latitude": float(sensor["latitude"]),
                "longitude": float(sensor["longitude"]),
                "temperatura": float(sensor.get("temperatura") or 0.0),
                "umidade": float(sensor.get("umidade") or 0.0),
                "co2": float(sensor.get("co2") or 0.0),
                "indice_risco": float(sensor.get("indice_risco") or 0.0),
                "risco_mapa": risk_label,
                "color": color,
                "radius": 40,
            }
        )
    return enriched


def build_map(sensors: list[dict]) -> pdk.Deck:
    center_lat = mean(sensor["latitude"] for sensor in sensors)
    center_lon = mean(sensor["longitude"] for sensor in sensors)

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=sensors,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_line_color=[20, 20, 20, 180],
        get_radius="radius",
        pickable=True,
        stroked=True,
        auto_highlight=True,
    )

    text = pdk.Layer(
        "TextLayer",
        data=sensors,
        get_position="[longitude, latitude]",
        get_text="sensor_id",
        get_color=[20, 20, 20, 220],
        get_size=12,
        get_alignment_baseline="'top'",
        get_pixel_offset=[0, 14],
    )

    return pdk.Deck(
        map_provider="carto",
        map_style="light",
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=11,
            pitch=0,
        ),
        layers=[scatter, text],
        tooltip={
            "html": """
                <b>Sensor:</b> {sensor_id}<br/>
                <b>Risco:</b> {risco_mapa}<br/>
                <b>Temperatura:</b> {temperatura} C<br/>
                <b>Umidade:</b> {umidade}%<br/>
                <b>CO2:</b> {co2}<br/>
                <b>IRI:</b> {indice_risco}<br/>
                <b>Status IA:</b> {status_ia_borda}<br/>
                <b>Timestamp:</b> {timestamp}
            """,
            "style": {"backgroundColor": "white", "color": "black"},
        },
    )


def count_by_risk(sensors: list[dict]) -> tuple[int, int, int]:
    sem_risco = sum(sensor["risco_mapa"] == "Sem Risco" for sensor in sensors)
    provavel = sum(sensor["risco_mapa"] == "Provavel Risco" for sensor in sensors)
    iminente = sum(sensor["risco_mapa"] == "Risco Iminente" for sensor in sensors)
    return sem_risco, provavel, iminente


st.set_page_config(
    page_title="Monitoramento de Sensores e Risco de Incendio",
    page_icon="fire",
    layout="wide",
)

st.title("Monitoramento Geografico de Sensores com Analise de Risco")
st.caption(
    "A aplicacao consulta o PostgreSQL em tempo quase real, posiciona os sensores no mapa e classifica o risco de incendio por cor."
)

with st.sidebar:
    st.header("Configuracoes")
    auto_refresh = st.checkbox("Atualizar automaticamente", value=True)
    refresh_seconds = st.slider(
        "Intervalo de atualizacao (segundos)",
        min_value=2,
        max_value=30,
        value=DEFAULT_REFRESH_SECONDS,
    )
    limit = st.slider(
        "Maximo de sensores exibidos",
        min_value=10,
        max_value=5000,
        value=DEFAULT_LIMIT,
        step=10,
    )
    probable_threshold = st.slider(
        "Limiar para provavel risco (IRI)",
        min_value=1,
        max_value=100,
        value=20,
    )
    imminent_threshold = st.slider(
        "Limiar para risco iminente (IRI)",
        min_value=1,
        max_value=100,
        value=80,
    )

    st.markdown("### Legenda")
    st.markdown("- Azul: sem risco")
    st.markdown("- Amarelo: provavel risco")
    st.markdown("- Vermelho: risco iminente")

if auto_refresh:
    schedule_refresh(refresh_seconds)

try:
    raw_sensors = fetch_latest_sensor_data(limit)
except Exception as exc:
    st.error(f"Erro ao consultar o PostgreSQL: {exc}")
    st.info(
        "Verifique se o Docker, o PostgreSQL e o consumidor Spark estao em execucao, e se a tabela historico_sensores possui dados."
    )
    st.stop()

if not raw_sensors:
    st.warning("Nenhum dado encontrado na tabela historico_sensores.")
    st.info("Execute o simulador e o consumidor Spark para popular o banco.")
    st.stop()

sensors = enrich_sensor_data(raw_sensors, probable_threshold, imminent_threshold)
sem_risco, provavel, iminente = count_by_risk(sensors)

metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
metric_col_1.metric("Sensores monitorados", len(sensors))
metric_col_2.metric("Sem risco", sem_risco)
metric_col_3.metric("Provavel risco", provavel)
metric_col_4.metric("Risco iminente", iminente)

info_col_1, info_col_2, info_col_3 = st.columns(3)
info_col_1.metric(
    "Temperatura maxima",
    f"{max(sensor['temperatura'] for sensor in sensors):.2f} C",
)
info_col_2.metric(
    "IRI medio",
    f"{mean(sensor['indice_risco'] for sensor in sensors):.2f}",
)
info_col_3.metric(
    "Ultima atualizacao",
    str(max(sensor["timestamp"] for sensor in sensors)),
)

st.pydeck_chart(build_map(sensors), use_container_width=True)

left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Ultimas leituras por sensor")
    st.dataframe(
        [
            {
                "sensor_id": sensor["sensor_id"],
                "timestamp": sensor["timestamp"],
                "temperatura": sensor["temperatura"],
                "umidade": sensor["umidade"],
                "co2": sensor["co2"],
                "indice_risco": sensor["indice_risco"],
                "status_ia_borda": sensor["status_ia_borda"],
                "risco_mapa": sensor["risco_mapa"],
            }
            for sensor in sensors
        ],
        use_container_width=True,
        hide_index=True,
    )

with right_col:
    st.subheader("Sensores em atencao")
    risky_sensors = [
        sensor
        for sensor in sensors
        if sensor["risco_mapa"] in {"Provavel Risco", "Risco Iminente"}
    ]
    if risky_sensors:
        st.dataframe(
            [
                {
                    "sensor_id": sensor["sensor_id"],
                    "risco": sensor["risco_mapa"],
                    "temperatura": sensor["temperatura"],
                    "IRI": sensor["indice_risco"],
                    "status_ia_borda": sensor["status_ia_borda"],
                }
                for sensor in risky_sensors
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("Nenhum sensor em nivel de atencao no momento.")
