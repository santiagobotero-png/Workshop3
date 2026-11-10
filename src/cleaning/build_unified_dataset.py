from pathlib import Path
import pandas as pd


# ============================================================
# 1. CONFIGURACIÓN DEL PROYECTO
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

YEARS = [2015, 2016, 2017, 2018, 2019]

OUTPUT_FILE = PROCESSED_DIR / "world_happiness_2015_2019.csv"


# ============================================================
# 2. ESQUEMA CANÓNICO
# ============================================================

CANONICAL_SCHEMA = {
    "year": "int64",
    "country": "string",
    "happiness_rank": "Int64",
    "happiness_score": "float64",
    "gdp": "float64",
    "social_support": "float64",
    "health": "float64",
    "freedom": "float64",
    "corruption": "float64",
    "generosity": "float64"
}


# ============================================================
# 3. MAPEO DE COLUMNAS POR EDICIÓN
# ============================================================

COLUMN_MAPPING = {
    2015: {
        "country": "Country",
        "happiness_rank": "Happiness Rank",
        "happiness_score": "Happiness Score",
        "gdp": "Economy (GDP per Capita)",
        "social_support": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "corruption": "Trust (Government Corruption)",
        "generosity": "Generosity"
    },

    2016: {
        "country": "Country",
        "happiness_rank": "Happiness Rank",
        "happiness_score": "Happiness Score",
        "gdp": "Economy (GDP per Capita)",
        "social_support": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "corruption": "Trust (Government Corruption)",
        "generosity": "Generosity"
    },

    2017: {
        "country": "Country",
        "happiness_rank": "Happiness.Rank",
        "happiness_score": "Happiness.Score",
        "gdp": "Economy..GDP.per.Capita.",
        "social_support": "Family",
        "health": "Health..Life.Expectancy.",
        "freedom": "Freedom",
        "corruption": "Trust..Government.Corruption.",
        "generosity": "Generosity"
    },

    2018: {
        "country": "Country or region",
        "happiness_rank": "Overall rank",
        "happiness_score": "Score",
        "gdp": "GDP per capita",
        "social_support": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "corruption": "Perceptions of corruption",
        "generosity": "Generosity"
    },

    2019: {
        "country": "Country or region",
        "happiness_rank": "Overall rank",
        "happiness_score": "Score",
        "gdp": "GDP per capita",
        "social_support": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "corruption": "Perceptions of corruption",
        "generosity": "Generosity"
    }
}


# ============================================================
# 4. VALIDACIÓN DEL MAPEO CONTRA LOS ARCHIVOS ORIGINALES
# ============================================================

def validate_source_columns():

    print("=" * 65)
    print("VALIDACIÓN DEL MAPEO DE COLUMNAS")
    print("=" * 65)

    all_valid = True

    for year in YEARS:

        file_path = RAW_DIR / f"{year}.csv"

        if not file_path.exists():
            print(f"\n[ERROR] No se encontró el archivo: {file_path}")
            all_valid = False
            continue

        source_columns = set(
            pd.read_csv(file_path, nrows=0).columns
        )

        expected_columns = set(COLUMN_MAPPING[year].values())

        missing_columns = expected_columns - source_columns

        if missing_columns:
            print(f"\n[ERROR] Año {year}")
            print(f"Columnas no encontradas: {sorted(missing_columns)}")
            all_valid = False

        else:
            print(f"\n[OK] Año {year}")
            print(f"Columnas mapeadas: {len(expected_columns)}")

    print("\n" + "=" * 65)

    if all_valid:
        print("RESULTADO: Todos los mapeos son válidos.")
    else:
        print("RESULTADO: Se encontraron problemas en los mapeos.")

    print("=" * 65)

    return all_valid

# ============================================================
# 5. LECTURA Y ESTANDARIZACIÓN DE CADA EDICIÓN
# ============================================================

def load_and_standardize_year(year):

    file_path = RAW_DIR / f"{year}.csv"

    # Leer archivo original
    df = pd.read_csv(
        file_path,
        na_values=["N/A", "NA", "n/a", "na", "NULL", "null", ""]
    )

    # Obtener correspondencia de columnas
    mapping = COLUMN_MAPPING[year]

    # Seleccionar y renombrar columnas
    df = df[list(mapping.values())].rename(
        columns={
            source: canonical
            for canonical, source in mapping.items()
        }
    )

    # Incorporar año de referencia
    df.insert(0, "year", year)

    # Normalizar espacios en nombres de países
    df["country"] = (
        df["country"]
        .astype("string")
        .str.strip()
    )

    # Convertir tipos numéricos
    numeric_columns = [
        "happiness_rank",
        "happiness_score",
        "gdp",
        "social_support",
        "health",
        "freedom",
        "corruption",
        "generosity"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="raise"
        )

    # Aplicar tipos definidos en el esquema canónico
    for column, dtype in CANONICAL_SCHEMA.items():
        df[column] = df[column].astype(dtype)

    # Ordenar columnas
    df = df[list(CANONICAL_SCHEMA.keys())]

    return df


# ============================================================
# 6. VALIDACIÓN DE LA ESTANDARIZACIÓN INDIVIDUAL
# ============================================================

def validate_standardized_datasets():

    print("=" * 65)
    print("VALIDACIÓN DE DATASETS ESTANDARIZADOS")
    print("=" * 65)

    standardized_datasets = {}

    for year in YEARS:

        df = load_and_standardize_year(year)

        standardized_datasets[year] = df

        print(f"\nAño: {year}")
        print(f"Registros: {len(df)}")
        print(f"Columnas: {len(df.columns)}")
        print(f"Esquema correcto: {list(df.columns) == list(CANONICAL_SCHEMA.keys())}")
        print(f"Año consistente: {df['year'].eq(year).all()}")
        print(f"Países faltantes: {df['country'].isna().sum()}")
        print(f"Duplicados exactos: {df.duplicated().sum()}")

        print("\nTipos de datos:")
        print(df.dtypes)

    print("\n" + "=" * 65)
    print("ESTANDARIZACIÓN INDIVIDUAL COMPLETADA")
    print("=" * 65)

    return standardized_datasets


# ============================================================
# 7. CONCATENACIÓN DE LAS CINCO EDICIONES
# ============================================================

def build_unified_dataset(standardized_datasets):

    print("=" * 65)
    print("CONSTRUCCIÓN DEL DATASET UNIFICADO")
    print("=" * 65)

    # Concatenar los cinco datasets
    df_unified = pd.concat(
        standardized_datasets.values(),
        ignore_index=True
    )

    # Garantizar el orden del esquema canónico
    df_unified = df_unified[
        list(CANONICAL_SCHEMA.keys())
    ]

    print(f"\nTotal de registros: {len(df_unified)}")
    print(f"Total de columnas: {len(df_unified.columns)}")

    return df_unified


# ============================================================
# 8. VALIDACIÓN DEL DATASET UNIFICADO
# ============================================================

def validate_unified_dataset(df):

    print("\n" + "=" * 65)
    print("VALIDACIÓN DEL DATASET UNIFICADO")
    print("=" * 65)

    expected_records = sum([
        158, 157, 155, 156, 156
    ])

    # --------------------------------------------------------
    # 1. Validación estructural
    # --------------------------------------------------------

    print("\n1. VALIDACIÓN ESTRUCTURAL")

    print(f"Registros esperados: {expected_records}")
    print(f"Registros obtenidos: {len(df)}")

    print(
        "Cantidad de registros correcta:",
        len(df) == expected_records
    )

    print(
        "Esquema correcto:",
        list(df.columns) == list(CANONICAL_SCHEMA.keys())
    )

    print(
        "Tipos de datos correctos:",
        all(
            str(df[column].dtype) == dtype
            for column, dtype in CANONICAL_SCHEMA.items()
        )
    )

    # --------------------------------------------------------
    # 2. Distribución de registros por año
    # --------------------------------------------------------

    print("\n2. REGISTROS POR AÑO")

    print(
        df.groupby("year")
        .size()
        .to_string()
    )

    # --------------------------------------------------------
    # 3. Validación de duplicados
    # --------------------------------------------------------

    print("\n3. VALIDACIÓN DE DUPLICADOS")

    exact_duplicates = df.duplicated().sum()

    duplicate_keys = df.duplicated(
        subset=["year", "country"]
    ).sum()

    print(f"Duplicados exactos: {exact_duplicates}")
    print(f"Duplicados año-país: {duplicate_keys}")

    # --------------------------------------------------------
    # 4. Valores faltantes
    # --------------------------------------------------------

    print("\n4. VALORES FALTANTES")

    print(
        df.isna()
        .sum()
        .to_string()
    )

    # --------------------------------------------------------
    # 5. Países vacíos
    # --------------------------------------------------------

    empty_countries = (
        df["country"]
        .str.strip()
        .eq("")
        .sum()
    )

    print("\n5. VALIDACIÓN DE PAÍSES")
    print(f"Nombres de países vacíos: {empty_countries}")

    # --------------------------------------------------------
    # 6. Inspección de valores de salud iguales a cero
    # --------------------------------------------------------

    print("\n6. INSPECCIÓN DE ANOMALÍAS")

    zero_health = df.loc[
        df["health"].eq(0),
        ["year", "country", "health"]
    ]

    print("Registros con health = 0:")

    if zero_health.empty:
        print("No se encontraron registros.")
    else:
        print(zero_health.to_string(index=False))

    # --------------------------------------------------------
    # 7. Resultado general
    # --------------------------------------------------------

    validation_passed = (
        len(df) == expected_records
        and list(df.columns) == list(CANONICAL_SCHEMA.keys())
        and all(
            str(df[column].dtype) == dtype
            for column, dtype in CANONICAL_SCHEMA.items()
        )
        and exact_duplicates == 0
        and duplicate_keys == 0
        and empty_countries == 0
    )

    print("\n" + "=" * 65)

    if validation_passed:
        print("RESULTADO: Dataset unificado validado correctamente.")
    else:
        print("RESULTADO: Se encontraron problemas de validación.")

    print("=" * 65)

    return validation_passed


# ============================================================
# 9. AUDITORÍA DE VALORES ANÓMALOS
# ============================================================

def audit_anomalous_values(df):

    print("=" * 65)
    print("AUDITORÍA DE VALORES ANÓMALOS")
    print("=" * 65)

    # --------------------------------------------------------
    # 1. Registros con health igual a cero
    # --------------------------------------------------------

    zero_health = df.loc[
        df["health"].eq(0),
        [
            "year",
            "country",
            "happiness_rank",
            "happiness_score",
            "gdp",
            "social_support",
            "health",
            "freedom",
            "corruption",
            "generosity"
        ]
    ].sort_values(["country", "year"])

    print("\n1. REGISTROS CON HEALTH = 0")

    if zero_health.empty:
        print("No se encontraron registros.")
    else:
        print(zero_health.to_string(index=False))

    # --------------------------------------------------------
    # 2. Trayectoria de los países afectados
    # --------------------------------------------------------

    affected_countries = zero_health["country"].unique()

    print("\n2. TRAYECTORIA DE LOS PAÍSES AFECTADOS")

    country_history = df.loc[
        df["country"].isin(affected_countries),
        [
            "year",
            "country",
            "happiness_score",
            "health"
        ]
    ].sort_values(["country", "year"])

    print(country_history.to_string(index=False))

    # --------------------------------------------------------
    # 3. Valores faltantes
    # --------------------------------------------------------

    print("\n3. REGISTROS CON VALORES FALTANTES")

    missing_values = df.loc[
        df.isna().any(axis=1)
    ]

    if missing_values.empty:
        print("No se encontraron valores faltantes.")
    else:
        print(missing_values.to_string(index=False))

    print("\n" + "=" * 65)
    print("AUDITORÍA COMPLETADA")
    print("=" * 65)

# ============================================================
# 11. DIAGNÓSTICO DE NOMBRES DE PAÍSES
# ============================================================

import re
import unicodedata


def normalize_country_key(country):

    # Normalizar caracteres Unicode
    country = unicodedata.normalize("NFKD", country)

    # Eliminar marcas diacríticas
    country = "".join(
        char for char in country
        if not unicodedata.combining(char)
    )

    # Convertir a minúsculas
    country = country.lower()

    # Sustituir puntuación por espacios
    country = re.sub(r"[^\w\s]", " ", country)

    # Eliminar espacios repetidos
    country = re.sub(r"\s+", " ", country).strip()

    return country


def audit_country_names(df):

    print("=" * 65)
    print("DIAGNÓSTICO DE NOMBRES DE PAÍSES")
    print("=" * 65)

    # Crear copia auxiliar
    countries = df[["country"]].drop_duplicates().copy()

    # Generar clave normalizada
    countries["normalized_key"] = (
        countries["country"]
        .apply(normalize_country_key)
    )

    # Identificar claves asociadas a varios nombres originales
    variants = (
        countries.groupby("normalized_key")
        .agg(
            number_of_names=("country", "nunique"),
            original_names=(
                "country",
                lambda values: list(values)
            )
        )
        .reset_index()
    )

    variants = variants.loc[
        variants["number_of_names"] > 1
    ]

    print("\n1. VARIANTES DETECTADAS AUTOMÁTICAMENTE")

    if variants.empty:
        print("No se encontraron variantes por normalización textual.")
    else:
        print(variants.to_string(index=False))

    # Mostrar inventario de países por año
    print("\n2. CANTIDAD DE PAÍSES POR AÑO")

    print(
        df.groupby("year")["country"]
        .nunique()
        .to_string()
    )

    # Mostrar nombres que requieren revisión explícita
    print("\n3. INVENTARIO DE NOMBRES ORIGINALES")

    print(
        countries.sort_values("normalized_key")
        .to_string(index=False)
    )

    print("\n" + "=" * 65)
    print("DIAGNÓSTICO COMPLETADO")
    print("=" * 65)


# ============================================================
# 13. IDENTIFICACIÓN TEMPORAL DE VARIANTES
# ============================================================

def inspect_country_variants_by_year(df):

    print("=" * 65)
    print("IDENTIFICACIÓN TEMPORAL DE VARIANTES")
    print("=" * 65)

    candidate_names = [
        "Somaliland region",
        "Somaliland Region",
        "Macedonia",
        "North Macedonia",
        "Hong Kong",
        "Hong Kong S.A.R., China",
        "Taiwan",
        "Taiwan Province of China",
        "Trinidad and Tobago",
        "Trinidad & Tobago",
        "North Cyprus",
        "Northern Cyprus",
        "Swaziland",
        "Eswatini"
    ]

    result = df.loc[
        df["country"].isin(candidate_names),
        ["year", "country"]
    ].copy()

    summary = (
        result.groupby(["country", "year"])
        .size()
        .unstack(fill_value=0)
    )

    # Garantizar que aparezcan todos los años
    summary = summary.reindex(
        columns=YEARS,
        fill_value=0
    )

    print("\nFrecuencia de cada denominación por año:\n")

    print(summary.to_string())

    print("\n" + "=" * 65)
    print("INSPECCIÓN COMPLETADA")
    print("=" * 65)

# ============================================================
# 15. DICCIONARIO DE EQUIVALENCIAS GEOGRÁFICAS
# ============================================================

COUNTRY_MAPPING = {

    # Northern Cyprus
    "North Cyprus": "Northern Cyprus",
    "Northern Cyprus": "Northern Cyprus",

    # Somaliland
    "Somaliland region": "Somaliland Region",

    # Macedonia
    "Macedonia": "North Macedonia",
    "North Macedonia": "North Macedonia",

    # Hong Kong
    "Hong Kong": "Hong Kong",
    "Hong Kong S.A.R., China": "Hong Kong",

    # Taiwan
    "Taiwan": "Taiwan",
    "Taiwan Province of China": "Taiwan",

    # Trinidad and Tobago
    "Trinidad and Tobago": "Trinidad and Tobago",
    "Trinidad & Tobago": "Trinidad and Tobago",

    # Swaziland
    "Swaziland": "Eswatini"
}


# ============================================================
# 16. NORMALIZACIÓN DE NOMBRES DE PAÍSES
# ============================================================

def normalize_country_names(df):

    print("=" * 65)
    print("NORMALIZACIÓN DE NOMBRES DE PAÍSES")
    print("=" * 65)

    df = df.copy()

    # Conservar denominación original
    df["country_original"] = df["country"]

    # Aplicar equivalencias explícitas
    df["country"] = (
        df["country"]
        .replace(COUNTRY_MAPPING)
    )

    print("\nEquivalencias aplicadas:")

    for original, canonical in COUNTRY_MAPPING.items():

        if original != canonical:

            count = (
                df["country_original"]
                .eq(original)
                .sum()
            )

            if count > 0:
                print(
                    f"{original} -> {canonical}: {count} registros"
                )

    print("\nNormalización completada.")

    return df


# ============================================================
# 17. VALIDACIÓN DE NORMALIZACIÓN GEOGRÁFICA
# ============================================================

def validate_country_normalization(df):

    print("=" * 65)
    print("VALIDACIÓN DE NORMALIZACIÓN GEOGRÁFICA")
    print("=" * 65)

    duplicated_keys = df.loc[
        df.duplicated(
            subset=["year", "country"],
            keep=False
        )
    ].sort_values(["year", "country"])

    if duplicated_keys.empty:

        print("\n[OK] No existen duplicados año-país.")

    else:

        print("\n[ERROR] Se encontraron colisiones:")

        print(
            duplicated_keys[
                ["year", "country_original", "country"]
            ].to_string(index=False)
        )

    print("\nPaíses originales:", df["country_original"].nunique())
    print("Países canónicos:", df["country"].nunique())

    validation_passed = duplicated_keys.empty

    print("\n" + "=" * 65)

    if validation_passed:
        print("RESULTADO: Normalización validada.")
    else:
        print("RESULTADO: Revisar equivalencias.")

    print("=" * 65)

    return validation_passed


# ============================================================
# 17. VALIDACIÓN DE NORMALIZACIÓN GEOGRÁFICA
# ============================================================

def validate_country_normalization(df):

    print("=" * 65)
    print("VALIDACIÓN DE NORMALIZACIÓN GEOGRÁFICA")
    print("=" * 65)

    duplicated_keys = df.loc[
        df.duplicated(
            subset=["year", "country"],
            keep=False
        )
    ].sort_values(["year", "country"])

    expected_records = 782

    print(f"\nRegistros esperados: {expected_records}")
    print(f"Registros obtenidos: {len(df)}")

    print(
        "\nDuplicados año-país:",
        df.duplicated(subset=["year", "country"]).sum()
    )

    print(
        "Países originales:",
        df["country_original"].nunique()
    )

    print(
        "Países canónicos:",
        df["country"].nunique()
    )

    print("\nDistribución por año:")

    print(
        df.groupby("year")
        .size()
        .to_string()
    )

    if not duplicated_keys.empty:

        print("\n[ERROR] Colisiones detectadas:")

        print(
            duplicated_keys[
                ["year", "country_original", "country"]
            ].to_string(index=False)
        )

    validation_passed = (
        len(df) == expected_records
        and duplicated_keys.empty
        and df["country"].notna().all()
        and df["country"].str.strip().ne("").all()
    )

    print("\n" + "=" * 65)

    if validation_passed:
        print("RESULTADO: Normalización geográfica validada.")
    else:
        print("RESULTADO: Revisar problemas de normalización.")

    print("=" * 65)

    return validation_passed


# ============================================================
# 18. EXPORTACIÓN DEL DATASET UNIFICADO
# ============================================================

def save_unified_dataset(df):

    print("=" * 65)
    print("EXPORTACIÓN DEL DATASET UNIFICADO")
    print("=" * 65)

    # Conservar exclusivamente el esquema canónico.
    # country_original se mantiene como columna auxiliar
    # durante la auditoría, pero no forma parte del dataset final.

    df_final = df[
        list(CANONICAL_SCHEMA.keys())
    ].copy()

    # Crear directorio si no existe
    PROCESSED_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # Exportar CSV
    df_final.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8"
    )

    print(f"\nArchivo generado: {OUTPUT_FILE}")
    print(f"Registros exportados: {len(df_final)}")
    print(f"Columnas exportadas: {len(df_final.columns)}")

    print("\nColumnas:")

    print(df_final.dtypes.to_string())

    print("\n" + "=" * 65)
    print("EXPORTACIÓN COMPLETADA")
    print("=" * 65)

    return df_final

# ============================================================
# 19. EJECUCIÓN
# ============================================================

if __name__ == "__main__":

    if validate_source_columns():

        datasets = validate_standardized_datasets()

        df_unified = build_unified_dataset(datasets)

        if validate_unified_dataset(df_unified):

            audit_anomalous_values(df_unified)

            audit_country_names(df_unified)

            inspect_country_variants_by_year(df_unified)

            df_normalized = normalize_country_names(df_unified)

            if validate_country_normalization(df_normalized):

                df_final = save_unified_dataset(df_normalized)