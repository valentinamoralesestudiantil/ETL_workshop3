# se utiliza para la limpieza y transformacion para entrenar al modelo 
import pandas as pd

def cargar_limpiar_unir_happiness(archivos):
    
    renombrar_columnas = {
        "Country": "country",
        "Country or region": "country",

        "Happiness Rank": "happiness_rank",
        "Happiness.Rank": "happiness_rank",
        "Overall rank": "happiness_rank",

        "Happiness Score": "happiness_score",
        "Happiness.Score": "happiness_score",
        "Score": "happiness_score",

        "Economy (GDP per Capita)": "gdp",
        "Economy..GDP.per.Capita.": "gdp",
        "GDP per capita": "gdp",

        "Family": "social_support",
        "Social support": "social_support",

        "Health (Life Expectancy)": "health",
        "Health..Life.Expectancy.": "health",
        "Healthy life expectancy": "health",

        "Freedom": "freedom",
        "Freedom to make life choices": "freedom",

        "Trust (Government Corruption)": "corruption",
        "Trust..Government.Corruption.": "corruption",
        "Perceptions of corruption": "corruption",

        "Generosity": "generosity"
    }

    columnas_finales = [
        "year",
        "country",
        "happiness_rank",
        "happiness_score",
        "gdp",
        "social_support",
        "health",
        "freedom",
        "generosity",
        "corruption"
    ]

    dataframes = []

    for year, ruta in archivos.items():
        df = pd.read_csv(ruta)

        # Limpiar nombres de columnas
        df.columns = df.columns.str.strip()

        # Renombrar columnas
        df = df.rename(columns=renombrar_columnas)

        # Agregar año
        df["year"] = year

        # Eliminar filas con faltantes antes de unir
        df = df.dropna(subset=["corruption"])

        # Crear columnas faltantes
        for col in columnas_finales:
            if col not in df.columns:
                df[col] = pd.NA

        # Dejar solo columnas finales
        df = df[columnas_finales]

        dataframes.append(df)

    # Unir todos los datasets
    df_unido = pd.concat(dataframes, ignore_index=True)

    # Convertir columnas numéricas
    columnas_numericas = [
        "year",
        "happiness_rank",
        "happiness_score",
        "gdp",
        "social_support",
        "health",
        "freedom",
        "generosity",
        "corruption"
    ]

    for col in columnas_numericas:
        df_unido[col] = pd.to_numeric(df_unido[col], errors="coerce")

    return df_unido