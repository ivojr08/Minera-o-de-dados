Este repositório apresenta todo o pipeline de mineração de dados — da coleta ao painel interativo — aplicado aos acidentes de trânsito em rodovias federais do Brasil de 2021 a 2024.

Objetivos:

- Praticar Git para controle de versão.

- Executar limpeza e validação de CSVs heterogêneos.

- Realizar EDA (Exploratory Data Analysis) com Python.

- Construir um app Streamlit com visualizações interativas.

Estrutura do projeto:

data/
    Dados/ # CSVs originais
    processed/ # Parquets (bruto, clean, feat)
src/
    build_dataset.py # ingestão + concatenação
    cleaning_dataset.py # imputação, duplicatas
    feature_engineering.py # novas colunas derivadas
app/
    eda_app.py # painel Streamlit

requirements.txt # dependências
README.md

Testado em Python 3.13 em Windows 11, no VSCODE

Clone o repositório:
 - git clone https://github.com/ivojr08/Minera-o-de-dados.git
 - cd Minera-o-de-dados

Crie e ative um ambiente virtual:
 - python -m venv .venv

Instale as dependências:

 - pip install -r requirements.txt

Gere os conjuntos Parquet:

# une os CSVs
python -m src.build_dataset
# valida / limpa
python -m src.cleaning_dataset
# engenharia de atributos
python -m src.feature_engineering

Inicie o app Streamlit

 - streamlit run app/eda_app.py

Autor:
Ivo José Caregnato Junior