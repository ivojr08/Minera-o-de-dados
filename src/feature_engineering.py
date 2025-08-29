from pathlib import Path
import pandas as pd
import numpy as np

ROOT     = Path(__file__).resolve().parents[1]
IN_FILE  = ROOT / "data" / "processed" / "acidentes_2021_2024_clean.parquet"
OUT_FILE = ROOT / "data" / "processed" / "acidentes_2021_2024_feat.parquet"

# Funções auxiliares
# -------------------------------------------------------------------- #
def periodo_do_dia(hora: pd.Series) -> pd.Series:
    """
    Converte hora (datetime.time ou string HH:MM:SS) em categorias:
    MADRUGADA (0-5) | MANHÃ (6-11) | TARDE (12-17) | NOITE (18-23)
    """
    hh = (
        hora.astype(str) # '05:30:00'
            .str.slice(0, 2) # '05'
            .astype(float, errors='ignore')
            .fillna(-1) # valores ausentes viram -1
    )
    bins   = [-1, 5, 11, 17, 23]
    labels = ["MADRUGADA", "MANHÃ", "TARDE", "NOITE"]
    return pd.cut(hh, bins=bins, labels=labels)
# Pipeline principal
# -------------------------------------------------------------------- #
def main() -> None:
    print("Lendo dataset limpo …")
    df = pd.read_parquet(IN_FILE)

     # Renomeia colunas para evitar inconsistências
    rename_map = {
        "mortos": "mortes",
        "feridos_leves":  "feridos_leves",
        "feridos_leve":   "feridos_leves",
        "feridos_graves": "feridos_graves",
        "feridos_grave":  "feridos_graves",
    }
    df = df.rename(columns=rename_map)

    # --- Partes da data ------------------------------------------------
    df["dia_semana_pt"] = df["data"].dt.day_name(locale="pt_BR") # segunda-feira, etc.
    df["mes"]           = df["data"].dt.month
    df["ano"]           = df["data"].dt.year
    df["eh_fim_de_semana"] = df["data"].dt.weekday >= 5 # sáb/dom → True

    # --- Período do dia ------------------------------------------------
    df["periodo_dia"] = periodo_do_dia(df["hora"])

    # --- Total feridos e índice de gravidade --------------------------
    df["total_feridos"]   = df[["feridos_leves", "feridos_graves"]].sum(axis=1)
    df["indice_gravidade"] = df["mortes"] * 3 + df["total_feridos"]

    # --- bucketizar km ----------------------------------
    if "km" in df.columns:
        km_bins   = list(range(0, int(df["km"].max()) + 100, 100))
        km_labels = [f"{a}-{b}" for a, b in zip(km_bins[:-1], km_bins[1:])]
        df["km_bucket"] = pd.cut(df["km"], bins=km_bins, labels=km_labels)

    # --- Salvar ---------------------------------------------------------
    print("Salvando dataset com features …")
    df.to_parquet(OUT_FILE, index=False)
    print(f"Parquet salvo em {OUT_FILE.relative_to(ROOT)}")

if __name__ == "__main__":
    main()
