# Une todas las bases de datos en una sola para el entrenamiento del modelo

from pathlib import Path
from cleaning import cargar_limpiar_unir_happiness

# Carpeta donde está este archivo union.py
BASE_DIR = Path(__file__).resolve().parent

# Carpeta data
DATA_DIR = BASE_DIR / "data"

archivos = {
    2015: DATA_DIR / "2015.csv",
    2016: DATA_DIR / "2016.csv",
    2017: DATA_DIR / "2017.csv",
    2018: DATA_DIR / "2018.csv",
    2019: DATA_DIR / "2019.csv"
}

df_unido = cargar_limpiar_unir_happiness(archivos)

# Crear carpeta para datos procesados
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Columnas numéricas con decimales
columnas_decimales = [
    "happiness_score",
    "gdp",
    "social_support",
    "health",
    "freedom",
    "generosity",
    "corruption"
]

# Redondear máximo a 5 decimales
df_unido[columnas_decimales] = df_unido[columnas_decimales].round(5)

# Guardar dataset limpio y unido
ruta_salida = PROCESSED_DIR / "happiness_clean_unified_2015_2019.csv"

df_unido.to_csv(ruta_salida, index=False)

print("Dataset limpio y unido guardado correctamente en:")
print(ruta_salida)

print(df_unido.head())
print(df_unido.shape)
print(df_unido.columns)
print(df_unido.isnull().sum())
