import os
import warnings

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import psycopg2
import streamlit as st

warnings.filterwarnings("ignore")

PG_HOST = os.getenv("PG_HOST", "172.31.253.25")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "uconnect")
PG_USER = os.getenv("PG_USER", "ucnnct")
PG_PASSWORD = os.getenv("PG_PASSWORD", "ucnnct_pg_2024")
PG_SCHEMA = os.getenv("PG_SCHEMA", "datamart")

st.set_page_config(
    page_title="UConnect Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; max-width: 1400px; }
    [data-testid="metric-container"] {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: .9rem 1.1rem;
    }
    [data-testid="metric-container"] label {
        color: #64748B; font-size: .72rem; font-weight: 600;
        text-transform: uppercase; letter-spacing: .06em;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #0F172A; font-size: 1.4rem; font-weight: 700;
    }
    div[data-testid="stTabs"] button { font-weight: 500; font-size: .9rem; }
    hr { border-color: #E2E8F0; }
</style>
""",
    unsafe_allow_html=True,
)

_layout = go.Layout(
    font=dict(family="Inter, system-ui, sans-serif", size=12, color="#374151"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    title=dict(font=dict(size=13, color="#1E293B"), x=0, xanchor="left", pad=dict(b=8)),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=0, r=8, t=44, b=0),
    xaxis=dict(showgrid=False, zeroline=False, tickfont=dict(size=11)),
    yaxis=dict(gridcolor="#F1F5F9", zeroline=False, tickfont=dict(size=11)),
    hoverlabel=dict(bgcolor="white", bordercolor="#E2E8F0", font=dict(size=12)),
)
pio.templates["uc"] = go.layout.Template(layout=_layout)
pio.templates.default = "plotly_white+uc"
CFG = {"displayModeBar": False}


def fmt_num(n) -> str:
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "—"
    n = float(n)
    if n >= 1_000_000_000:
        return f"{n/1e9:.1f}G"
    if n >= 1_000_000:
        return f"{n/1e6:.1f}M"
    if n >= 1_000:
        return f"{n/1e3:.1f}K"
    return f"{n:.0f}"


def fmt_pct(r, d=1) -> str:
    if r is None or (isinstance(r, float) and pd.isna(r)):
        return "—"
    return f"{float(r)*100:.{d}f}%"


def fmt_f(v, d=2) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.{d}f}"


def area_chart(df, x, y, title, color="#3B82F6"):
    fig = px.area(
        df,
        x=x,
        y=y,
        title=title,
        color_discrete_sequence=[color],
        markers=True,
        labels={x: "", y: ""},
    )
    fig.update_layout(showlegend=False, height=300)
    fig.update_traces(line=dict(width=2))
    return fig


@st.cache_resource
def _pg():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


@st.cache_data(ttl=300)
def load(q: str) -> pd.DataFrame:
    return pd.read_sql(q, _pg())


# ── Requêtes datamart ──────────────────────────────────────────────────────────

# Stats globales (header) — agrégé sur toutes les sources, snapshot le plus récent
SQL_STATS = f"""
SELECT
    SUM(total_events)                                                          AS total_events,
    SUM(total_acteurs)                                                         AS total_acteurs,
    ROUND(SUM(dau_moyen * total_events) / NULLIF(SUM(total_events), 0), 2)    AS dau_moyen,
    ROUND(SUM(total_events)::numeric / NULLIF(SUM(total_acteurs), 0), 2)      AS avg_events_per_user,
    ROUND(
        SUM(COALESCE(resolution_rate, 0) * total_events)::numeric
        / NULLIF(SUM(total_events), 0), 4
    )                                                                          AS resolution_rate,
    MIN(first_event_ts)                                                        AS first_event_ts,
    MAX(last_event_ts)                                                         AS last_event_ts
FROM {PG_SCHEMA}.stats_globales
WHERE period_end = (SELECT MAX(period_end) FROM {PG_SCHEMA}.stats_globales)
"""

# KPI 1 par jour — moyenne des sources (toutes les périodes historiques)
SQL_KPI1 = f"""
SELECT
    period_start,
    AVG(taux_actifs)        AS taux_actifs,
    AVG(taux_participation) AS taux_participation,
    AVG(taux_fonctionnel)   AS taux_fonctionnel,
    AVG(freq_moyenne)       AS freq_moyenne
FROM {PG_SCHEMA}.kpi1_utilisation
GROUP BY period_start
ORDER BY period_start DESC
"""

# KPI 2 par jour — moyenne/somme des sources
SQL_KPI2 = f"""
SELECT
    period_start,
    AVG(taux_participation_groupe) AS taux_participation_groupe,
    AVG(taux_resolution)           AS taux_resolution,
    AVG(interaction_moyenne)       AS interaction_moyenne,
    SUM(nb_messages_total)         AS nb_messages_total
FROM {PG_SCHEMA}.kpi2_collaboration
GROUP BY period_start
ORDER BY period_start DESC
"""

# Volumétrie par source — ligne la plus récente par source
SQL_SOURCES = f"""
SELECT DISTINCT ON (source)
    source,
    total_events        AS staging_lignes,
    total_acteurs       AS staging_acteurs,
    raw_size_bytes,
    raw_file_count,
    curated_size_bytes,
    curated_file_count,
    staging_rows,
    staging_size_bytes,
    computed_at
FROM {PG_SCHEMA}.stats_globales
ORDER BY source, computed_at DESC
"""

stats = load(SQL_STATS)
kpi1 = load(SQL_KPI1)
kpi2 = load(SQL_KPI2)
sources = load(SQL_SOURCES)

row = stats.iloc[0]
first = str(row["first_event_ts"])[:10] if pd.notna(row["first_event_ts"]) else ""
last = str(row["last_event_ts"])[:10] if pd.notna(row["last_event_ts"]) else ""

with st.sidebar:
    periods = sorted(kpi1["period_start"].astype(str).unique(), reverse=True)
    period = st.selectbox("Période", periods, index=0) if periods else None

k1 = (
    kpi1[kpi1["period_start"].astype(str) == period].iloc[0]
    if period and not kpi1.empty
    else None
)
k2 = (
    kpi2[kpi2["period_start"].astype(str) == period].iloc[0]
    if period and not kpi2.empty
    else None
)

# ── Header ──────────────────────────────────────────────────────────────────────
col_t, col_d = st.columns([4, 1])
col_t.markdown("## UConnect Analytics")
if first and last:
    col_d.markdown(
        f"<div style='text-align:right;color:#94A3B8;font-size:.82rem;padding-top:1.1rem'>"
        f"{first} → {last}</div>",
        unsafe_allow_html=True,
    )
st.divider()

# ── Cartes globales ──────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Événements", fmt_num(row["total_events"]))
c2.metric("Acteurs distincts", fmt_num(row["total_acteurs"]))
c3.metric("DAU moyen", fmt_num(row["dau_moyen"]))
c4.metric("Events / acteur", fmt_f(row["avg_events_per_user"]))
c5.metric("Taux résolution", fmt_pct(row["resolution_rate"]))

st.divider()

tab_global, tab_kpi1, tab_kpi2, tab_evol, tab_raw = st.tabs(
    [
        "Vue globale",
        "KPI 1 — Utilisation",
        "KPI 2 — Collaboration",
        "Évolution",
        "Données brutes",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# VUE GLOBALE — volumétrie par couche et par source
# ══════════════════════════════════════════════════════════════════════════════
with tab_global:
    if not sources.empty:
        raw_total = sources["raw_size_bytes"].sum()
        curated_total = sources["curated_size_bytes"].sum()
        staging_total = sources["staging_rows"].sum()
        staging_sz = sources["staging_size_bytes"].sum()

        ca, cb, cc, cd = st.columns(4)
        ca.metric("Raw", f"{raw_total / 1024**3:.1f} Go")
        cb.metric("Curated", f"{curated_total / 1024**3:.1f} Go")
        cc.metric("Staging lignes", fmt_num(staging_total))
        cd.metric("Staging taille", f"{staging_sz / 1024**3:.1f} Go")

        st.divider()

        df_vol = sources.copy()
        df_vol["Raw (Go)"] = (df_vol["raw_size_bytes"] / 1024**3).round(2)
        df_vol["Raw (fichiers)"] = df_vol["raw_file_count"]
        df_vol["Curated (Go)"] = (df_vol["curated_size_bytes"] / 1024**3).round(2)
        df_vol["Curated (fichiers)"] = df_vol["curated_file_count"]
        df_vol["Staging (lignes)"] = df_vol["staging_lignes"]
        df_vol["Staging (acteurs)"] = df_vol["staging_acteurs"]
        df_vol["Màj"] = df_vol["computed_at"].astype(str).str[:16]

        st.dataframe(
            df_vol[
                [
                    "source",
                    "Raw (Go)",
                    "Raw (fichiers)",
                    "Curated (Go)",
                    "Curated (fichiers)",
                    "Staging (lignes)",
                    "Staging (acteurs)",
                    "Màj",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.divider()

        fig_src = px.bar(
            df_vol.sort_values("staging_lignes"),
            x="staging_lignes",
            y="source",
            orientation="h",
            title="Lignes en staging par source",
            labels={"staging_lignes": "", "source": ""},
            color_discrete_sequence=["#3B82F6"],
            text="staging_lignes",
        )
        fig_src.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside",
            cliponaxis=False,
        )
        fig_src.update_layout(
            height=280,
            xaxis=dict(showticklabels=False),
            yaxis=dict(autorange="reversed"),
            margin=dict(l=0, r=80, t=44, b=0),
        )
        st.plotly_chart(fig_src, use_container_width=True, config=CFG)


# ══════════════════════════════════════════════════════════════════════════════
# KPI 1 — UTILISATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_kpi1:
    if k1 is not None:
        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Taux actifs", fmt_pct(k1["taux_actifs"], 2))
        col_b.metric("Taux participation", fmt_pct(k1["taux_participation"], 2))
        col_c.metric("Taux fonctionnel", fmt_pct(k1["taux_fonctionnel"], 2))
        col_d.metric("Fréq. moyenne", f"{fmt_f(k1['freq_moyenne'])} evt/acteur")

    st.divider()

    if not kpi1.empty and len(kpi1) > 1:
        df_k1 = kpi1.sort_values("period_start").copy()
        df_k1["taux_actifs_pct"] = df_k1["taux_actifs"] * 100
        df_k1["taux_fonctionnel_pct"] = df_k1["taux_fonctionnel"] * 100

        col_e, col_f = st.columns(2, gap="large")

        with col_e:
            fig = area_chart(
                df_k1, "period_start", "taux_actifs_pct", "Taux actifs (%)", "#3B82F6"
            )
            fig.update_layout(yaxis=dict(ticksuffix="%", range=[0, 60]))
            st.plotly_chart(fig, use_container_width=True, config=CFG)

        with col_f:
            fig2 = area_chart(
                df_k1,
                "period_start",
                "freq_moyenne",
                "Fréquence moyenne (evt / acteur / jour)",
                "#8B5CF6",
            )
            st.plotly_chart(fig2, use_container_width=True, config=CFG)


# ══════════════════════════════════════════════════════════════════════════════
# KPI 2 — COLLABORATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_kpi2:
    if k2 is not None:
        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric(
            "Participation groupe", fmt_pct(k2["taux_participation_groupe"], 2)
        )
        col_b.metric("Taux résolution", fmt_pct(k2["taux_resolution"], 2))
        col_c.metric(
            "Interaction moyenne", f"{fmt_f(k2['interaction_moyenne'])} msg/acteur"
        )
        col_d.metric("Total messages", fmt_num(k2["nb_messages_total"]))

    st.divider()

    if not kpi2.empty and len(kpi2) > 1:
        df_k2 = kpi2.sort_values("period_start").copy()
        df_k2["taux_pg_pct"] = df_k2["taux_participation_groupe"] * 100

        col_g, col_h = st.columns(2, gap="large")

        with col_g:
            fig3 = area_chart(
                df_k2,
                "period_start",
                "taux_pg_pct",
                "Participation groupe (%)",
                "#3B82F6",
            )
            fig3.update_layout(yaxis=dict(ticksuffix="%", range=[0, 60]))
            st.plotly_chart(fig3, use_container_width=True, config=CFG)

        with col_h:
            fig4 = area_chart(
                df_k2,
                "period_start",
                "interaction_moyenne",
                "Interaction moyenne (msg / acteur)",
                "#10B981",
            )
            st.plotly_chart(fig4, use_container_width=True, config=CFG)

        fig5 = area_chart(
            df_k2,
            "period_start",
            "nb_messages_total",
            "Volume messages / jour",
            "#F59E0B",
        )
        st.plotly_chart(fig5, use_container_width=True, config=CFG)


# ══════════════════════════════════════════════════════════════════════════════
# ÉVOLUTION
# ══════════════════════════════════════════════════════════════════════════════
with tab_evol:
    df_k1_s = kpi1.sort_values("period_start").copy()
    df_k2_s = kpi2.sort_values("period_start").copy()

    if len(df_k1_s) >= 2 and len(df_k2_s) >= 2:
        # ── Messages + taux actifs (dual axis) ──────────────────────────────
        df_merge = df_k1_s.merge(
            df_k2_s[["period_start", "nb_messages_total"]],
            on="period_start",
            how="inner",
        )
        df_merge["taux_actifs_pct"] = df_merge["taux_actifs"] * 100

        fig_vol = go.Figure()
        fig_vol.add_trace(
            go.Scatter(
                x=df_merge["period_start"],
                y=df_merge["nb_messages_total"],
                name="Messages / jour",
                mode="lines+markers",
                line=dict(color="#3B82F6", width=2),
                fill="tozeroy",
                fillcolor="rgba(59,130,246,0.08)",
            )
        )
        fig_vol.add_trace(
            go.Scatter(
                x=df_merge["period_start"],
                y=df_merge["taux_actifs_pct"],
                name="Taux actifs (%)",
                mode="lines+markers",
                line=dict(color="#8B5CF6", width=2),
                yaxis="y2",
            )
        )
        fig_vol.update_layout(
            title="Volume et utilisation / jour",
            height=320,
            yaxis=dict(
                title="Messages",
                gridcolor="#F1F5F9",
                zeroline=False,
                tickfont=dict(size=11),
            ),
            yaxis2=dict(
                title="Taux actifs (%)",
                overlaying="y",
                side="right",
                zeroline=False,
                tickfont=dict(size=11),
                ticksuffix="%",
            ),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            margin=dict(l=0, r=60, t=44, b=0),
        )
        st.plotly_chart(fig_vol, use_container_width=True, config=CFG)

        st.divider()

        col_3, col_4 = st.columns(2, gap="large")

        df_k1_s["taux_actifs_pct"] = df_k1_s["taux_actifs"] * 100
        with col_3:
            fig_ta = area_chart(
                df_k1_s, "period_start", "taux_actifs_pct", "Taux actifs (%)", "#3B82F6"
            )
            fig_ta.update_layout(yaxis=dict(ticksuffix="%", range=[0, 60]))
            st.plotly_chart(fig_ta, use_container_width=True, config=CFG)

        with col_4:
            fig_fm = area_chart(
                df_k1_s,
                "period_start",
                "freq_moyenne",
                "Fréquence moyenne (evt / acteur / jour)",
                "#F59E0B",
            )
            st.plotly_chart(fig_fm, use_container_width=True, config=CFG)

        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Pic messages / jour", fmt_num(df_k2_s["nb_messages_total"].max()))
        c2.metric("Moy. taux actifs", fmt_pct(df_k1_s["taux_actifs"].mean(), 2))
        c3.metric("Moy. fréquence", fmt_f(df_k1_s["freq_moyenne"].mean()))
        c4.metric("Nb jours", str(len(df_k1_s)))


# ══════════════════════════════════════════════════════════════════════════════
# DONNÉES BRUTES
# ══════════════════════════════════════════════════════════════════════════════
with tab_raw:
    s1, s2, s3, s4 = st.tabs(["Volumétrie", "KPI 1", "KPI 2", "Stats globales"])
    with s1:
        st.dataframe(sources, use_container_width=True, hide_index=True)
    with s2:
        st.dataframe(
            kpi1.sort_values("period_start", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    with s3:
        st.dataframe(
            kpi2.sort_values("period_start", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    with s4:
        st.dataframe(stats, use_container_width=True, hide_index=True)
