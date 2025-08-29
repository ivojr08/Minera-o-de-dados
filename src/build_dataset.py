from pathlib import Path
import pandas as pd
from unidecode import unidecode

# Pastas de entrada e saída
# ------------------------------------------------------------------ #
ROOT     = Path(__file__).resolve().parents[1]          # raiz do projeto
RAW_DIR  = ROOT / "data" / "Dados"
OUT_DIR  = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Funções auxiliares
# ------------------------------------------------------------------ #
def _read_csv_auto(path: Path) -> pd.DataFrame:
    """
    Tenta ler o CSV com várias combinações (sep, encoding, engine).
    - Usa engine 'c' primeiro (rápido, preciso).
    - Se falhar ou voltar só 1 coluna, tenta engine 'python', que é
      mais tolerante e permite pular linhas quebradas.
    """
    combos = [
        (";", "latin1",  "c"),
        (",", "utf-8",   "c"),
        (",", "latin1",  "c"),
        (";", "utf-8",   "python"),   # último recurso
    ]

    for sep, enc, eng in combos:
        try:
            if eng == "c":
                df = pd.read_csv(
                    path, sep=sep, encoding=enc, low_memory=False, engine="c"
                )
            else:  # engine python
                df = pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    engine="python",
                    on_bad_lines="skip"
                )
            # se veio tudo em uma coluna, provavelmente separador errado
            if df.shape[1] == 1:
                continue
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    raise ValueError(f"Falha ao ler {path} — cheque separador/encoding.")


def _clean_basic(df: pd.DataFrame) -> pd.DataFrame:
    
    # Pequenas padronizações que valem para todos os anos.
    
    # normaliza nomes de colunas
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    # data_inversa → datetime; horario → time
    if "data_inversa" in df.columns:
        df["data"] = pd.to_datetime(df["data_inversa"], dayfirst=True, errors="coerce")
    if "horario" in df.columns:
        df["hora"] = pd.to_datetime(df["horario"], format="%H:%M:%S", errors="coerce").dt.time

    # vírgula decimal → ponto
    for col in ("km", "latitude", "longitude"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace('"', "", regex=False)
                .astype("float64", errors="ignore")
            )

    # remove acentos de algumas colunas-chave
    for txt in ("municipio", "causa_acidente", "tipo_acidente", "classificacao_acidente"):
        if txt in df.columns:
            df[txt] = (
                df[txt]
                .astype(str)
                .apply(unidecode)
                .str.strip()
            )

    # descarta linhas sem data válida
    if "data" in df.columns:
        df = df.dropna(subset=["data"]).reset_index(drop=True)

    return df
    
# Pipeline principal
# ------------------------------------------------------------------ #
def build_parquet() -> Path:
    """Lê todos os CSVs de data/raw, consolida e grava Parquet; devolve o Path."""
    frames = []

    for csv in sorted(RAW_DIR.glob("*.csv")):
        print(f"Lendo {csv.name} …")
        df = _read_csv_auto(csv)
        df["ano"] = csv.stem[:4]        # 2021, 2022, …
        frames.append(df)

    print("Concatenando DataFrames …")
    full = pd.concat(frames, ignore_index=True)

    print("Limpeza básica …")
    full = _clean_basic(full)

    out_file = OUT_DIR / "acidentes_2021_2024.parquet"
    full.to_parquet(out_file, index=False)
    print(f"Parquet salvo em: {out_file.relative_to(ROOT)}")
    return out_file
    
# Execução via linha de comando
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    build_parquet()
