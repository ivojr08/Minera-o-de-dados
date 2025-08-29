from pathlib import Path
import pandas as pd
import numpy as np

ROOT      = Path(__file__).resolve().parents[1]
IN_FILE   = ROOT / "data" / "processed" / "acidentes_2021_2024.parquet"
OUT_FILE  = ROOT / "data" / "processed" / "acidentes_2021_2024_clean.parquet"

NUMERIC_IMPUTE    = "median"
CATEGORICAL_IMPUTE = "mode"

def main() -> None:
    # ------------------------------------------------------------------
    print("Lendo Parquet consolidado …")
    df = pd.read_parquet(IN_FILE)
    
    # Datas como datetime
    # ------------------------------------------------------------------
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
    if "hora" in df.columns:
        # hora já vem como datetime.time - mantém
        pass
    # Valores ausentes
    # ------------------------------------------------------------------
    print("\nNulos por coluna (antes do tratamento):")
    print(df.isna().sum().sort_values(ascending=False).head(12))

    # Separar dtypes
    num_cols  = df.select_dtypes(include=["number"]).columns
    cat_cols  = df.select_dtypes(include=["object", "category"]).columns

    # Imputação numérica
    if NUMERIC_IMPUTE == "mean":
        df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
    else:
        df[num_cols] = df[num_cols].fillna(df[num_cols].median())

    # Imputação categórica (moda)
    for col in cat_cols:
        moda = df[col].mode(dropna=True)
        if not moda.empty:
            df[col] = df[col].fillna(moda[0])
            
    # Remoção de duplicatas e registros incoerentes
    # ------------------------------------------------------------------
    antes = len(df)
    # duplicatas exatas
    df = df.drop_duplicates()

    # registros sem id ou data (incoerentes p/ análise temporal)
    if "id" in df.columns:
        df = df.dropna(subset=["id", "data"])
    else:
        df = df.dropna(subset=["data"])

    depois = len(df)
    print(f"\nRemovidas {antes - depois} linhas duplicadas/incoerentes.")
    
    # Relatório final
    # ------------------------------------------------------------------
    print("\nNulos por coluna (depois do tratamento):")
    print(df.isna().sum().sort_values(ascending=False).head(12))

    # ------------------------------------------------------------------
    print(f"\nSalvando Parquet limpo em {OUT_FILE.relative_to(ROOT)} …")
    df.to_parquet(OUT_FILE, index=False)
    print("Concluído.")

if __name__ == "__main__":
    main()
