#######################
# Import libraries
import os
import time

import duckdb
import joblib
import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px

from duckdb_load import get_data, get_time_series_data

#######################
# Page configuration
st.set_page_config(
    page_title="Incidentes",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

#######################
# CSS styling
st.markdown("""
<style>

[data-testid="block-container"] {
    padding-left: 2rem;
    padding-right: 2rem;
    padding-top: 1rem;
    padding-bottom: 0rem;
    margin-bottom: -7rem;
}

[data-testid="stVerticalBlock"] {
    padding-left: 0rem;
    padding-right: 0rem;
}

[data-testid="stMetric"] {
    background-color: #393939;
    text-align: center;
    padding: 15px 0;
}

[data-testid="stMetricLabel"] {
  display: flex;
  justify-content: center;
  align-items: center;
}

[data-testid="stMetricDeltaIcon-Up"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

[data-testid="stMetricDeltaIcon-Down"] {
    position: relative;
    left: 38%;
    -webkit-transform: translateX(-50%);
    -ms-transform: translateX(-50%);
    transform: translateX(-50%);
}

</style>
""", unsafe_allow_html=True)
load_from_minio = os.getenv('LOAD_FROM_MINO', False)
if load_from_minio:
    df = get_data()
else:
    df = duckdb.sql("SELECT * FROM output.csv").df()

st.title("Dashboard Gerencial de Incidentes e Alarmes")

df['alarm_time_minutes'] = (df['duracao_incidente'] / 60).astype(int)

time_counts = df['alarm_time_minutes'].value_counts().sort_index()

time_counts = time_counts.to_frame()
time_counts['value'] = time_counts.index

col1, col2, col3 = st.columns([1, 2, 2])

count_duration = df['duracao_incidente'].count()
col1.metric(label="Total de incidentes", value=round(count_duration, 2))

avg_duration = df['duracao_incidente'].mean()
col1.metric(label="Duração Média dos Incidentes (s)", value=round(avg_duration, 2))

total_incidentes_por_grupo = df['equipamento_grupo'].value_counts()

maior_grupo = total_incidentes_por_grupo.idxmax()
total_incidentes_maior_grupo = total_incidentes_por_grupo.max()

col1.metric(label=f"Total de Incidentes do Maior Grupo de Equipamentos: {maior_grupo}",
            value=total_incidentes_maior_grupo)

fig_equip_group = px.pie(df, names="equipamento_grupo", title="Incidentes por Grupo de equipamento")
col2.plotly_chart(fig_equip_group, use_container_width=True)

fig_turno = px.pie(df, names="turno", title="Incidentes por Turno")
col3.plotly_chart(fig_turno, use_container_width=True)

fig_stack_bar = px.histogram(
    df,
    x="alarm_time_minutes",
    color="alarme_codigo",
    title="Contagem de Alarme Código por Minutos para Resolução",
    labels={"alarm_time_minutes": "Minutos para Resolução", "count": "Contagem"},
    barmode="stack"
    , text_auto=True
)

st.plotly_chart(fig_stack_bar, use_container_width=True)


df_time = get_time_series_data()
st.subheader("Previsão de incidentes")
fig = px.line(df_time, x='date', y='pred', title='Forecast Incidentes em Novembro')

st.plotly_chart(fig, use_container_width=True)

st.subheader("Previsão de MTTR")
model = joblib.load("mttr_model.pkl")
encoder = joblib.load("encoder.pkl")

col_t, col_a, col_al, col_eq = st.columns(4)
with col_t:
    turno_input = st.selectbox("Selecione o Turno", ['1', '2', '3'])
with col_a:
    alarme_codigo_input = st.selectbox("Selecione o Código de Alarme", df['alarme_codigo'].unique())
with col_al:
    alarme_level_input = st.selectbox("Selecione o Nível de Alarme", ['Warning', 'Critical'])
with col_eq:
    equipamento_grupo_input = st.selectbox("Selecione o Grupo de Equipamento", df['equipamento_grupo'].unique())

if 'predictions_df' not in st.session_state:
    st.session_state['predictions_df'] = []


def get_data():
    return st.session_state.predictions_df


input_data = pd.DataFrame({
    "turno": [turno_input],
    "alarme_codigo": [alarme_codigo_input],
    "alarme_level": [alarme_level_input],
    "equipamento_grupo": [equipamento_grupo_input]
})

predictions_df = pd.DataFrame(columns=["turno", "alarme_codigo", "alarme_level", "equipamento_grupo", "mttr_pred"])

if st.button("Predict"):
    input_transformed = encoder.transform(input_data)
    predicted_mttr = model.predict(input_transformed)[0]
    new_row = input_data.assign(mttr_pred=predicted_mttr)
    get_data().append({
        "turno": [turno_input],
        "alarme_codigo": [alarme_codigo_input],
        "alarme_level": [alarme_level_input],
        "equipamento_grupo": [equipamento_grupo_input],
        "mttr_pred": [predicted_mttr]
    })

df = pd.DataFrame(get_data(), columns=["turno", "alarme_codigo", "alarme_level", "equipamento_grupo", "mttr_pred"])
st.dataframe(df, width=2000)
