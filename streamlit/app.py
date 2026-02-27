import warnings

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import psycopg2
import streamlit as st

warnings.filterwarnings("ignore")

# ── Connexion ──────────────────────────────────────────────────────────────────
PG_HOST = "172.31.253.25"
PG_PORT = 5432
PG_DB = "uconnect"
PG_USER = "ucnnct"
PG_PASSWORD = "ucnnct_pg_2024"
PG_SCHEMA = "datamart"

st.set_page_config(
    page_title="UConnect — Dashboard analytique",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Palette ────────────────────────────────────────────────────────────────────
COLORS = {
    "bluesky": "#0085FF",
    "nostr": "#8B5CF6",
    "hackernews": "#FF6600",
    "rss": "#10B981",
    "stackoverflow": "#E74C3C",
    "all": "#6B7280",
}

# ── Template Plotly global ─────────────────────────────────────────────────────
_tpl = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Inter, system-ui, sans-serif", size=13, color="#1F2937"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=dict(font=dict(size=15, color="#111827"), x=0, xanchor="left"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=48, r=24, t=56, b=40),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(gridcolor="#F3F4F6", zeroline=False),
        hoverlabel=dict(
            bgcolor="white",
            bordercolor="#E5E7EB",
            font=dict(family="Inter, system-ui, sans-serif", size=12),
        ),
    )
)
pio.templates["uc"] = _tpl
pio.templates.default = "plotly_white+uc"

CHART_CFG = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "toImageButtonOptions": {"format": "svg", "filename": "uconnect"},
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def fmt_number(n) -> str:
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "—"
    n = float(n)
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}G"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


def fmt_bytes(b) -> str:
    if b is None or (isinstance(b, float) and pd.isna(b)):
        return "—"
    b = float(b)
    if b >= 1e9:
        return f"{b/1e9:.1f} GB"
    if b >= 1e6:
        return f"{b/1e6:.1f} MB"
    return f"{b/1e3:.1f} KB"


def fmt_rate(r) -> str:
    if r is None or (isinstance(r, float) and pd.isna(r)):
        return "—"
    return f"{float(r)*100:.1f}%"


# ── Caching ────────────────────────────────────────────────────────────────────
@st.cache_resource
def _conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


@st.cache_data(ttl=300, max_entries=64)
def load(query: str) -> pd.DataFrame:
    return pd.read_sql(query, _conn())


# ── Chargement ─────────────────────────────────────────────────────────────────
stats = load(f"SELECT * FROM {PG_SCHEMA}.stats_globales ORDER BY period_start DESC, source")
kpi1 = load(f"SELECT * FROM {PG_SCHEMA}.kpi1_utilisation ORDER BY period_start DESC, source")
kpi2 = load(f"SELECT * FROM {PG_SCHEMA}.kpi2_collaboration ORDER BY period_start DESC, source")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")

    periods = sorted(stats["period_start"].astype(str).unique(), reverse=True)
    period = st.selectbox("Periode", periods, index=0)

    sources_dispo = sorted(
        stats[stats["period_start"].astype(str) == period]["source"].unique()
    )
    sources_sel = st.multiselect("Sources", sources_dispo, default=sources_dispo)

    st.divider()
    st.caption(f"Cache rafraichi toutes les 5 min.")

if not sources_sel:
    st.warning("Selectionnez au moins une source dans le filtre.")
    st.stop()

# ── Filtrage ───────────────────────────────────────────────────────────────────
s = stats[
    (stats["period_start"].astype(str) == period) & (stats["source"].isin(sources_sel))
]
k1 = kpi1[
    (kpi1["period_start"].astype(str) == period) & (kpi1["source"].isin(sources_sel))
]
k2 = kpi2[
    (kpi2["period_start"].astype(str) == period) & (kpi2["source"].isin(sources_sel))
]

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("UConnect — Tableau de bord analytique")
st.caption(f"Periode : {period}  |  Sources : {', '.join(sources_sel)}")

# ── KPI cards ──────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total evenements", fmt_number(s["total_events"].sum()))
c2.metric("Total acteurs", fmt_number(s["total_acteurs"].sum()))
c3.metric("DAU moyen", fmt_number(s["dau_moyen"].mean()))
c4.metric("Lac total (raw)", fmt_bytes(s["raw_size_bytes"].sum()))
c5.metric("Resolution rate", fmt_rate(s[s["resolution_rate"] > 0]["resolution_rate"].mean()))

st.divider()

# ── Tabs principaux ────────────────────────────────────────────────────────────
tab_vol, tab_rates, tab_kpi1, tab_kpi2, tab_temporal, tab_raw = st.tabs([
    "Volumetrie",
    "Comportements",
    "KPI 1 — Utilisation",
    "KPI 2 — Collaboration",
    "Evolution temporelle",
    "Donnees brutes",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — VOLUMETRIE
# ══════════════════════════════════════════════════════════════════════════════
with tab_vol:
    st.subheader("Volumetrie par source")

    scale = st.radio(
        "Echelle",
        ["Log (recommande)", "Lineaire", "Part relative (%)"],
        horizontal=True,
        key="scale_vol",
    )

    col_l, col_r = st.columns(2)

    # -- Evenements --
    with col_l:
        s_sorted = s.sort_values("total_events", ascending=True)

        if scale == "Lineaire":
            fig = px.bar(
                s_sorted,
                x="total_events",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                title="Total evenements",
                text=s_sorted["total_events"].apply(fmt_number),
            )
            fig.update_xaxes(title_text="Evenements")
        elif scale == "Log (recommande)":
            fig = px.bar(
                s_sorted,
                x="total_events",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                log_x=True,
                title="Total evenements (echelle log)",
                text=s_sorted["total_events"].apply(fmt_number),
            )
            fig.update_xaxes(title_text="Evenements (log10)")
        else:
            s_pct = s.copy()
            total = s_pct["total_events"].sum()
            s_pct["pct"] = s_pct["total_events"] / total * 100
            s_pct = s_pct.sort_values("pct", ascending=True)
            fig = px.bar(
                s_pct,
                x="pct",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                title="Part du total (%)",
                text=s_pct["pct"].apply(lambda x: f"{x:.1f}%"),
            )
            fig.update_xaxes(title_text="Part (%)", range=[0, 105])

        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    # -- Treemap distribution --
    with col_r:
        fig = px.treemap(
            s,
            path=["source"],
            values="total_events",
            color="source",
            color_discrete_map=COLORS,
            title="Distribution des evenements",
            hover_data={"total_events": ":,"},
        )
        fig.update_traces(
            texttemplate="%{label}<br>%{value:,}<br>%{percentRoot:.1%}",
            textposition="middle center",
            textfont_size=13,
        )
        fig.update_layout(margin=dict(t=56, l=0, r=0, b=0))
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    # -- Volumetrie du lac --
    st.subheader("Taille du lac par couche")
    s_lake = s.melt(
        id_vars="source",
        value_vars=["raw_size_bytes", "curated_size_bytes", "staging_size_bytes"],
        var_name="couche",
        value_name="taille",
    )
    s_lake["couche"] = s_lake["couche"].map({
        "raw_size_bytes": "Raw",
        "curated_size_bytes": "Curated",
        "staging_size_bytes": "Staging",
    })
    s_lake["taille_fmt"] = s_lake["taille"].apply(fmt_bytes)

    fig = px.bar(
        s_lake,
        x="source",
        y="taille",
        color="couche",
        barmode="group",
        title="Taille du lac (bytes) par source et couche",
        text="taille_fmt",
        color_discrete_sequence=["#93C5FD", "#3B82F6", "#1E40AF"],
    )
    fig.update_traces(textposition="outside")
    fig.update_yaxes(title_text="Taille (bytes)")
    st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    # -- Fichiers --
    col_a, col_b = st.columns(2)
    with col_a:
        fig = px.bar(
            s.sort_values("raw_file_count", ascending=True),
            x="raw_file_count",
            y="source",
            orientation="h",
            color="source",
            color_discrete_map=COLORS,
            title="Fichiers raw (MinIO)",
            text="raw_file_count",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)
    with col_b:
        fig = px.bar(
            s.sort_values("curated_file_count", ascending=True),
            x="curated_file_count",
            y="source",
            orientation="h",
            color="source",
            color_discrete_map=COLORS,
            title="Fichiers curated (Parquet)",
            text="curated_file_count",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — COMPORTEMENTS
# ══════════════════════════════════════════════════════════════════════════════
with tab_rates:
    st.subheader("Taux comportementaux par source")

    col_radar, col_bar = st.columns([1, 1])

    with col_radar:
        categories = ["Reply rate", "Thread rate", "Help request", "Resolution"]

        fig = go.Figure()
        for _, row in s.iterrows():
            source = row["source"]
            values = [
                float(row["reply_rate"] or 0) * 100,
                float(row["thread_rate"] or 0) * 100,
                float(row["help_request_rate"] or 0) * 100,
                float(row["resolution_rate"] or 0) * 100,
            ]
            values.append(values[0])
            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=categories + [categories[0]],
                fill="toself",
                name=source,
                opacity=0.65,
                line=dict(color=COLORS.get(source, "#6B7280"), width=2),
                fillcolor=COLORS.get(source, "#6B7280"),
            ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title="Comparaison des taux comportementaux (%)",
            legend=dict(orientation="v", x=1.05),
        )
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    with col_bar:
        rates = s[
            ["source", "reply_rate", "thread_rate", "help_request_rate", "resolution_rate"]
        ].melt(id_vars="source", var_name="indicateur", value_name="taux")
        rates["taux"] = rates["taux"].fillna(0).astype(float)
        rates["indicateur"] = rates["indicateur"].map({
            "reply_rate": "Reply rate",
            "thread_rate": "Thread rate",
            "help_request_rate": "Help request",
            "resolution_rate": "Resolution",
        })

        fig = px.bar(
            rates,
            x="indicateur",
            y="taux",
            color="source",
            barmode="group",
            title="Taux par indicateur et source",
            color_discrete_map=COLORS,
            labels={"taux": "Taux (0-1)", "indicateur": "Indicateur"},
        )
        fig.update_yaxes(range=[0, 1.05], title_text="Taux (0 = 0 %, 1 = 100 %)")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    # -- Acteurs et DAU --
    st.subheader("Acteurs et activite")
    col_c, col_d = st.columns(2)

    with col_c:
        fig = px.bar(
            s.sort_values("total_acteurs", ascending=True),
            x="total_acteurs",
            y="source",
            orientation="h",
            color="source",
            color_discrete_map=COLORS,
            title="Total acteurs distincts",
            text=s.sort_values("total_acteurs")["total_acteurs"].apply(fmt_number),
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    with col_d:
        fig = px.bar(
            s.sort_values("dau_moyen", ascending=True),
            x="dau_moyen",
            y="source",
            orientation="h",
            color="source",
            color_discrete_map=COLORS,
            title="DAU moyen (daily active users)",
            text=s.sort_values("dau_moyen")["dau_moyen"].apply(fmt_number),
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — KPI 1
# ══════════════════════════════════════════════════════════════════════════════
with tab_kpi1:
    st.subheader("KPI 1 — Utilisation du reseau")

    if k1.empty:
        st.info("Pas de donnees KPI 1 pour la periode et les sources selectionnees.")
    else:
        col_e, col_f = st.columns(2)

        with col_e:
            k1_rates = k1[
                ["source", "taux_actifs", "taux_participation", "taux_fonctionnel"]
            ].melt(id_vars="source", var_name="indicateur", value_name="taux")
            k1_rates["taux"] = k1_rates["taux"].fillna(0).astype(float)
            k1_rates["indicateur"] = k1_rates["indicateur"].map({
                "taux_actifs": "Actifs",
                "taux_participation": "Participation",
                "taux_fonctionnel": "Fonctionnel",
            })

            fig = px.bar(
                k1_rates,
                x="indicateur",
                y="taux",
                color="source",
                barmode="group",
                title="Taux d'utilisation par source",
                color_discrete_map=COLORS,
                labels={"taux": "Taux", "indicateur": "Indicateur"},
            )
            fig.update_yaxes(range=[0, 1.05], title_text="Taux (0 = 0 %, 1 = 100 %)")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

        with col_f:
            fig = px.bar(
                k1.sort_values("freq_moyenne", ascending=True),
                x="freq_moyenne",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                title="Frequence moyenne d'utilisation (jours actifs / total acteurs)",
                text=k1.sort_values("freq_moyenne")["freq_moyenne"].apply(
                    lambda x: f"{x:.2f}" if pd.notna(x) else "—"
                ),
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False, yaxis_title="")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — KPI 2
# ══════════════════════════════════════════════════════════════════════════════
with tab_kpi2:
    st.subheader("KPI 2 — Collaboration etudiante")

    if k2.empty:
        st.info("Pas de donnees KPI 2 pour la periode et les sources selectionnees.")
    else:
        col_g, col_h = st.columns(2)

        with col_g:
            k2_rates = k2[
                ["source", "taux_participation_groupe", "taux_resolution"]
            ].melt(id_vars="source", var_name="indicateur", value_name="taux")
            k2_rates["taux"] = k2_rates["taux"].fillna(0).astype(float)
            k2_rates["indicateur"] = k2_rates["indicateur"].map({
                "taux_participation_groupe": "Participation groupe",
                "taux_resolution": "Resolution",
            })

            fig = px.bar(
                k2_rates,
                x="indicateur",
                y="taux",
                color="source",
                barmode="group",
                title="Taux de collaboration par source",
                color_discrete_map=COLORS,
                labels={"taux": "Taux", "indicateur": "Indicateur"},
            )
            fig.update_yaxes(range=[0, 1.05], title_text="Taux (0 = 0 %, 1 = 100 %)")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

        with col_h:
            fig = px.bar(
                k2.sort_values("nb_messages_total", ascending=True),
                x="nb_messages_total",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                log_x=True,
                title="Nombre total de messages (echelle log)",
                text=k2.sort_values("nb_messages_total")["nb_messages_total"].apply(
                    fmt_number
                ),
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False, yaxis_title="")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

        # interaction_moyenne
        k2_clean = k2[k2["interaction_moyenne"].notna()].copy()
        if not k2_clean.empty:
            fig = px.bar(
                k2_clean.sort_values("interaction_moyenne", ascending=True),
                x="interaction_moyenne",
                y="source",
                orientation="h",
                color="source",
                color_discrete_map=COLORS,
                title="Interaction moyenne (messages par acteur actif par jour)",
                text=k2_clean.sort_values("interaction_moyenne")[
                    "interaction_moyenne"
                ].apply(lambda x: f"{x:.2f}"),
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False, yaxis_title="")
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — EVOLUTION TEMPORELLE
# ══════════════════════════════════════════════════════════════════════════════
with tab_temporal:
    st.subheader("Evolution temporelle")

    sources_tuple = tuple(sorted(sources_sel))
    stats_all = load(
        f"SELECT * FROM {PG_SCHEMA}.stats_globales"
        f" WHERE source = ANY(ARRAY{list(sources_tuple)})"
        f" ORDER BY period_start"
    )

    if stats_all.empty or len(stats_all["period_start"].unique()) < 2:
        st.info(
            "Pas assez de points temporels pour afficher une evolution. "
            "Le job stats_globales doit tourner plusieurs jours."
        )
    else:
        metric_choice = st.selectbox(
            "Metrique",
            ["total_events", "total_acteurs", "dau_moyen",
             "reply_rate", "resolution_rate"],
            format_func=lambda x: {
                "total_events": "Total evenements",
                "total_acteurs": "Total acteurs",
                "dau_moyen": "DAU moyen",
                "reply_rate": "Reply rate",
                "resolution_rate": "Resolution rate",
            }.get(x, x),
        )

        # Small multiples : echelles independantes
        fig = px.area(
            stats_all,
            x="period_start",
            y=metric_choice,
            facet_col="source",
            facet_col_wrap=3,
            color="source",
            color_discrete_map=COLORS,
            title=f"Evolution de '{metric_choice}' par source (echelles independantes)",
            markers=True,
        )
        fig.update_yaxes(matches=None, showticklabels=True)
        fig.update_xaxes(matches=None)
        fig.for_each_annotation(
            lambda a: a.update(text=a.text.split("=")[-1].title())
        )
        fig.update_layout(height=480, showlegend=False)
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

        # Vue consolidee (log pour eviter l'ecrasement)
        fig2 = px.line(
            stats_all,
            x="period_start",
            y=metric_choice,
            color="source",
            color_discrete_map=COLORS,
            log_y=True,
            title=f"Evolution consolidee (echelle log)",
            markers=True,
            labels={"period_start": "Date", metric_choice: "Valeur (log)"},
        )
        st.plotly_chart(fig2, use_container_width=True, config=CHART_CFG)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — DONNEES BRUTES
# ══════════════════════════════════════════════════════════════════════════════
with tab_raw:
    st.subheader("Donnees brutes")

    raw_tab1, raw_tab2, raw_tab3 = st.tabs([
        "Stats globales",
        "KPI 1 — Utilisation",
        "KPI 2 — Collaboration",
    ])
    with raw_tab1:
        st.dataframe(s.reset_index(drop=True), use_container_width=True)
    with raw_tab2:
        st.dataframe(k1.reset_index(drop=True), use_container_width=True)
    with raw_tab3:
        st.dataframe(k2.reset_index(drop=True), use_container_width=True)
