# app/eda_app.py
import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="EDA – Acidentes 2021-24", layout="wide")

# ──────────── Carregamento (cache) ────────────
@st.cache_data
def load_data():
    return pd.read_parquet("data/processed/acidentes_2021_2024_feat.parquet")

df = load_data()

# ──────────── Sidebar ─────────────────────────
st.sidebar.header("Filtros")
anos_sel = st.sidebar.multiselect(
    "Ano", sorted(df["ano"].unique()), default=sorted(df["ano"].unique())
)
ufs_sel = st.sidebar.multiselect(
    "UF", sorted(df["uf"].unique()), default=sorted(df["uf"].unique())
)
tempo_sel = st.sidebar.multiselect(
    "Condição do tempo",
    sorted(df["condicao_metereologica"].unique()),
    default=sorted(df["condicao_metereologica"].unique()),
)

f = df.query(
    "ano in @anos_sel and uf in @ufs_sel and condicao_metereologica in @tempo_sel"
)

# ──────────── KPIs ────────────────────────────
c1, c2, c3 = st.columns(3)
c1.metric("Acidentes", f.shape[0])
c2.metric("Mortes", int(f["mortes"].sum()))
c3.metric("Feridos", int(f["total_feridos"].sum()))

st.divider()

# ──────────── Tabs ────────────────────────────
tab_dist, tab_top5, tab_mapa, tab_disp = st.tabs(
    ["Distribuições", "Top 5", "Mapa", "Dispersão"]
)

# 1) DISTRIBUIÇÕES
with tab_dist:
    st.subheader("Histograma & Boxplot")
    var = st.selectbox(
        "Variável numérica",
        ["km", "total_feridos", "mortes", "indice_gravidade"],
        index=1,
        key="var_dist",
    )

    col_a, col_b = st.columns([1, 1], gap="small")

    # Histograma
    with col_a:
        fig_h = px.histogram(f, x=var, nbins=40, title=f"Histograma – {var}")
        fig_h.update_layout(
            width=450, height=320, margin=dict(l=0, r=0, t=40, b=0)
        )
        st.plotly_chart(fig_h, use_container_width=True)

    # Boxplot
    with col_b:
        fig_b = px.box(f, y=var, points="outliers", title=f"Boxplot – {var}")
        fig_b.update_layout(
            width=450, height=320, margin=dict(l=0, r=0, t=40, b=0)
        )
        st.plotly_chart(fig_b, use_container_width=True)

    # Matriz de correlação
    st.subheader("Matriz de correlação")
    num_cols = ["km", "total_feridos", "mortes", "indice_gravidade"]
    corr = f[num_cols].corr()
    fig_c = px.imshow(
        corr, text_auto=True, color_continuous_scale="RdBu_r", aspect="auto"
    )
    fig_c.update_layout(
        autosize=True, height=350, margin=dict(l=0, r=0, t=40, b=0)
    )
    st.plotly_chart(fig_c, use_container_width=True)

# 2) TOP 5
with tab_top5:
    st.subheader("Top 5 Causas")
    top_causa = (
        f["causa_acidente"].value_counts().head(5).reset_index()
    )
    top_causa.columns = ["causa_acidente", "contagem"]

    fig_causa = px.bar(
        top_causa,
        x="contagem",
        y="causa_acidente",
        orientation="h",
        title="Top 5 Causas de Acidente",
    )
    fig_causa.update_layout(
        width=500, height=350, margin=dict(l=80, r=20, t=40, b=0)
    )
    st.plotly_chart(fig_causa, use_container_width=True)

    st.subheader("Top 5 Tipos de Acidente")
    top_tipo = (
        f["tipo_acidente"].value_counts().head(5).reset_index()
    )
    top_tipo.columns = ["tipo_acidente", "contagem"]

    fig_tipo = px.bar(
        top_tipo,
        x="contagem",
        y="tipo_acidente",
        orientation="h",
        title="Top 5 Tipos de Acidente",
        color="tipo_acidente",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_tipo.update_layout(
        width=500, height=350, margin=dict(l=80, r=20, t=40, b=0),
        showlegend=False,
    )
    st.plotly_chart(fig_tipo, use_container_width=True)

# 3) MAPA
with tab_mapa:
    st.subheader("Mapa de Acidentes (amostra máx. 5 000 pontos)")
    if {"latitude", "longitude"}.issubset(f.columns):
        df_sample = f.sample(min(5000, len(f)), random_state=42)
        fig_m = px.scatter_mapbox(
            df_sample,
            lat="latitude",
            lon="longitude",
            hover_data={"municipio": True, "data": True, "mortes": True},
            zoom=3,
            height=600,
        )
        fig_m.update_layout(
            mapbox_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0)
        )
        st.plotly_chart(fig_m, use_container_width=True)
    else:
        st.info("Não há colunas latitude/longitude disponíveis.")

# 4) DISPERSÃO
with tab_disp:
    st.subheader("Nº de vítimas × Condição do tempo")
    y_var = st.selectbox(
        "Métrica de vítimas",
        ["mortes", "total_feridos", "indice_gravidade"],
        index=2,
        key="y_disp",
    )
    fig_s = px.strip(
        f,
        x="condicao_metereologica",
        y=y_var,
        color="condicao_metereologica",
        hover_data=["data", "municipio"],
        title=f"{y_var} por condição do tempo",
    )
    fig_s.update_layout(
        autosize=True, height=450, margin=dict(l=0, r=0, t=40, b=0),
        showlegend=False,
    )
    st.plotly_chart(fig_s, use_container_width=True)
