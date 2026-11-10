
from pathlib import Path
import logging
import re
import unicodedata

import numpy as np
import pandas as pd


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CARPETA_RAW = PROJECT_ROOT / "data" / "raw"
CARPETA_REPORTES = PROJECT_ROOT / "reports" / "profiling"

ANIOS = [2015, 2016, 2017, 2018, 2019]

ARCHIVOS = {
    anio: CARPETA_RAW / f"{anio}.csv"
    for anio in ANIOS
}

ENCODINGS = ["utf-8-sig", "cp1252", "latin1"]

# Valores que deben interpretarse como nulos.
VALORES_NULOS = [
    "N/A",
    "NA",
    "n/a",
    "na",
    "NULL",
    "null",
    "",
]

# Columnas equivalentes entre las diferentes versiones del dataset.
ALIAS_COLUMNAS = {
    "country": [
        "Country",
        "Country or region",
    ],
    "region": [
        "Region",
    ],
    "rank": [
        "Happiness Rank",
        "Happiness.Rank",
        "Overall rank",
    ],
    "score": [
        "Happiness Score",
        "Happiness.Score",
        "Score",
    ],
}

# Variables para las que no tiene sentido aplicar
# una detección estadística de outliers mediante IQR.
COLUMNAS_EXCLUIDAS_IQR = {
    "Happiness Rank",
    "Happiness.Rank",
    "Overall rank",
    "Lower Confidence Interval",
    "Upper Confidence Interval",
    "Whisker.high",
    "Whisker.low",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# 2. FUNCIONES GENERALES
# ============================================================

def leer_csv(ruta):
    """
    Lee un CSV intentando diferentes codificaciones.
    No modifica el archivo original.
    """

    errores = []

    for encoding in ENCODINGS:
        try:
            df = pd.read_csv(
                ruta,
                encoding=encoding,
                na_values=VALORES_NULOS,
                keep_default_na=True,
            )

            logger.info(
                "Archivo leído: %s | Encoding: %s",
                ruta.name,
                encoding,
            )

            return df

        except UnicodeDecodeError as error:
            errores.append(str(error))

    raise ValueError(
        f"No fue posible leer {ruta.name}. "
        f"Errores: {errores}"
    )


def normalizar_texto(valor):
    """
    Normaliza textos para comparaciones.

    Ejemplos:
    'United Arab Emirates' -> 'united arab emirates'
    '  Colombia  '         -> 'colombia'

    Elimina diferencias de mayúsculas, espacios,
    acentos y puntuación.
    """

    if pd.isna(valor):
        return None

    texto = str(valor).strip().casefold()

    texto = unicodedata.normalize("NFKD", texto)

    texto = "".join(
        caracter
        for caracter in texto
        if not unicodedata.combining(caracter)
    )

    texto = re.sub(r"[^a-z0-9]+", " ", texto)

    texto = re.sub(r"\s+", " ", texto)

    return texto.strip()


def obtener_columna(df, tipo):
    """
    Identifica una columna usando sus posibles alias.
    """

    aliases = ALIAS_COLUMNAS.get(tipo, [])

    for columna in aliases:
        if columna in df.columns:
            return columna

    return None


def obtener_nombre_pais(df):
    return obtener_columna(df, "country")


def obtener_registros_numericos(df):
    return df.select_dtypes(include=[np.number]).columns.tolist()


# ============================================================
# 3. CARGA DE DATASETS
# ============================================================

def cargar_datasets():
    datasets = {}

    for anio, ruta in ARCHIVOS.items():

        if not ruta.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo: {ruta}"
            )

        df = leer_csv(ruta)

        datasets[anio] = df

        logger.info(
            "%s | Filas: %s | Columnas: %s",
            anio,
            len(df),
            len(df.columns),
        )

    return datasets


# ============================================================
# 4. INVENTARIO DE DATASETS
# ============================================================

def generar_inventario(datasets):

    resultados = []

    for anio, df in datasets.items():

        resultados.append({
            "year": anio,
            "file_name": ARCHIVOS[anio].name,
            "rows": len(df),
            "columns": len(df.columns),
            "memory_mb": round(
                df.memory_usage(deep=True).sum() / (1024 ** 2),
                4,
            ),
        })

    return pd.DataFrame(resultados)


# ============================================================
# 5. PERFILADO INDIVIDUAL DE COLUMNAS
# ============================================================

def generar_perfil_columnas(datasets):

    resultados = []

    for anio, df in datasets.items():

        for columna in df.columns:

            serie = df[columna]

            registro = {
                "year": anio,
                "column": columna,
                "dtype": str(serie.dtype),
                "total_records": len(serie),
                "non_null": int(serie.notna().sum()),
                "missing": int(serie.isna().sum()),
                "missing_pct": round(
                    serie.isna().mean() * 100,
                    2,
                ),
                "unique_values": int(serie.nunique()),
            }

            if pd.api.types.is_numeric_dtype(serie):

                registro.update({
                    "min": serie.min(),
                    "max": serie.max(),
                    "mean": serie.mean(),
                    "median": serie.median(),
                    "std": serie.std(),
                    "q1": serie.quantile(0.25),
                    "q3": serie.quantile(0.75),
                })

            else:

                valores = (
                    serie.dropna()
                    .astype(str)
                    .unique()[:5]
                )

                registro.update({
                    "min": None,
                    "max": None,
                    "mean": None,
                    "median": None,
                    "std": None,
                    "q1": None,
                    "q3": None,
                    "sample_values": " | ".join(valores),
                })

            resultados.append(registro)

    return pd.DataFrame(resultados)


# ============================================================
# 6. COMPARACIÓN DE ESQUEMAS
# ============================================================

def generar_comparacion_esquemas(datasets):

    todas_columnas = sorted({
        columna
        for df in datasets.values()
        for columna in df.columns
    })

    resultados = []

    for columna in todas_columnas:

        registro = {
            "column": columna,
        }

        for anio, df in datasets.items():

            registro[str(anio)] = (
                columna in df.columns
            )

        registro["years_present"] = sum(
            columna in df.columns
            for df in datasets.values()
        )

        resultados.append(registro)

    return pd.DataFrame(resultados)


# ============================================================
# 7. DETALLE DE VALORES FALTANTES
# ============================================================

def generar_detalle_faltantes(datasets):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)

        for columna in df.columns:

            indices = df.index[df[columna].isna()]

            for indice in indices:

                pais = (
                    df.loc[indice, columna_pais]
                    if columna_pais
                    else None
                )

                resultados.append({
                    "year": anio,
                    "row_index": int(indice),
                    "country": pais,
                    "column": columna,
                    "missing_value": "N/A / NULL / blank",
                })

    return pd.DataFrame(resultados)


# ============================================================
# 8. VALIDACIÓN DE DUPLICADOS
# ============================================================

def validar_duplicados_paises(datasets):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)

        if not columna_pais:
            continue

        temporal = df.copy()

        temporal["_country_normalized"] = (
            temporal[columna_pais].apply(normalizar_texto)
        )

        duplicados = temporal[
            temporal["_country_normalized"].notna()
            & temporal.duplicated(
                subset=["_country_normalized"],
                keep=False,
            )
        ]

        for clave, grupo in duplicados.groupby(
            "_country_normalized"
        ):

            resultados.append({
                "year": anio,
                "normalized_country": clave,
                "country_variants": " | ".join(
                    sorted(
                        grupo[columna_pais]
                        .dropna()
                        .astype(str)
                        .unique()
                    )
                ),
                "records": len(grupo),
            })

    return pd.DataFrame(resultados)


# ============================================================
# 9. VALIDACIÓN DE EMPATES EN RANKINGS
# ============================================================

def validar_empates_ranking(datasets):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)
        columna_rank = obtener_columna(df, "rank")

        if not columna_rank:
            continue

        columnas = [columna_rank]

        if columna_pais:
            columnas.append(columna_pais)

        temporal = df[columnas].copy()

        temporal[columna_rank] = pd.to_numeric(
            temporal[columna_rank],
            errors="coerce",
        )

        duplicados = temporal[
            temporal[columna_rank].notna()
            & temporal.duplicated(
                subset=[columna_rank],
                keep=False,
            )
        ]

        for ranking, grupo in duplicados.groupby(
            columna_rank
        ):

            resultados.append({
                "year": anio,
                "ranking": ranking,
                "countries": " | ".join(
                    grupo[columna_pais]
                    .dropna()
                    .astype(str)
                    .tolist()
                ) if columna_pais else None,
                "records": len(grupo),
            })

    return pd.DataFrame(resultados)


# ============================================================
# 10. VALIDACIÓN DE EMPATES EN HAPPINESS SCORE
# ============================================================

def validar_empates_score(datasets):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)
        columna_score = obtener_columna(df, "score")

        if not columna_score:
            continue

        columnas = [columna_score]

        if columna_pais:
            columnas.append(columna_pais)

        temporal = df[columnas].copy()

        temporal[columna_score] = pd.to_numeric(
            temporal[columna_score],
            errors="coerce",
        )

        duplicados = temporal[
            temporal[columna_score].notna()
            & temporal.duplicated(
                subset=[columna_score],
                keep=False,
            )
        ]

        for puntuacion, grupo in duplicados.groupby(
            columna_score
        ):

            resultados.append({
                "year": anio,
                "happiness_score": puntuacion,
                "countries": " | ".join(
                    grupo[columna_pais]
                    .dropna()
                    .astype(str)
                    .tolist()
                ) if columna_pais else None,
                "records": len(grupo),
            })

    return pd.DataFrame(resultados)


# ============================================================
# 11. COBERTURA DE PAÍSES ENTRE AÑOS
# ============================================================

def generar_cobertura_paises(datasets):

    cobertura = {}

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)

        if not columna_pais:
            continue

        for pais in df[columna_pais].dropna().unique():

            clave = normalizar_texto(pais)

            if not clave:
                continue

            if clave not in cobertura:

                cobertura[clave] = {
                    "normalized_country": clave,
                    "country_variants": set(),
                    "years": set(),
                }

            cobertura[clave]["country_variants"].add(
                str(pais).strip()
            )

            cobertura[clave]["years"].add(anio)

    resultados = []

    for clave, datos in cobertura.items():

        anios_presentes = sorted(datos["years"])

        anios_ausentes = [
            anio
            for anio in ANIOS
            if anio not in anios_presentes
        ]

        registro = {
            "normalized_country": clave,
            "country_variants": " | ".join(
                sorted(datos["country_variants"])
            ),
            "years_present": " | ".join(
                map(str, anios_presentes)
            ),
            "years_missing": " | ".join(
                map(str, anios_ausentes)
            ),
            "years_count": len(anios_presentes),
            "present_in_all_years": (
                len(anios_presentes) == len(ANIOS)
            ),
        }

        for anio in ANIOS:
            registro[str(anio)] = (
                anio in datos["years"]
            )

        resultados.append(registro)

    return pd.DataFrame(resultados)


# ============================================================
# 12. VALIDACIÓN DE CONFLICTOS DE REGIÓN
# ============================================================

def validar_conflictos_region(datasets):

    regiones_por_pais = {}

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)
        columna_region = obtener_columna(df, "region")

        if not columna_pais or not columna_region:
            continue

        temporal = df[
            [columna_pais, columna_region]
        ].dropna()

        for _, fila in temporal.iterrows():

            pais = normalizar_texto(
                fila[columna_pais]
            )

            region = normalizar_texto(
                fila[columna_region]
            )

            if not pais or not region:
                continue

            if pais not in regiones_por_pais:
                regiones_por_pais[pais] = {
                    "country_variants": set(),
                    "regions": {},
                }

            regiones_por_pais[pais][
                "country_variants"
            ].add(str(fila[columna_pais]).strip())

            regiones_por_pais[pais]["regions"].setdefault(
                region,
                set(),
            ).add(anio)

    resultados = []

    for pais, datos in regiones_por_pais.items():

        regiones = datos["regions"]

        if len(regiones) > 1:

            detalle_regiones = []

            for region, anios in regiones.items():

                detalle_regiones.append(
                    f"{region}: {sorted(anios)}"
                )

            resultados.append({
                "normalized_country": pais,
                "country_variants": " | ".join(
                    sorted(datos["country_variants"])
                ),
                "regions_found": len(regiones),
                "region_details": " | ".join(
                    detalle_regiones
                ),
            })

    return pd.DataFrame(resultados)


# ============================================================
# 13. VALIDACIÓN DE VALORES INFINITOS Y RANGOS
# ============================================================

def validar_anomalias_numericas(datasets):

    resultados = []

    for anio, df in datasets.items():

        columnas_numericas = obtener_registros_numericos(df)

        columna_pais = obtener_nombre_pais(df)
        columna_rank = obtener_columna(df, "rank")
        columna_score = obtener_columna(df, "score")

        for columna in columnas_numericas:

            serie = pd.to_numeric(
                df[columna],
                errors="coerce",
            )

            for indice, valor in serie.items():

                if pd.isna(valor):
                    continue

                pais = (
                    df.loc[indice, columna_pais]
                    if columna_pais
                    else None
                )

                # --------------------------------------------
                # Valores infinitos
                # --------------------------------------------

                if np.isinf(valor):

                    resultados.append({
                        "year": anio,
                        "country": pais,
                        "column": columna,
                        "value": valor,
                        "anomaly_type": "INFINITE_VALUE",
                        "detail": "El valor es positivo o negativo infinito.",
                    })

                    continue

                # --------------------------------------------
                # Valores negativos
                # --------------------------------------------

                if valor < 0:

                    resultados.append({
                        "year": anio,
                        "country": pais,
                        "column": columna,
                        "value": valor,
                        "anomaly_type": "NEGATIVE_VALUE",
                        "detail": "Se identificó un valor negativo.",
                    })

                # --------------------------------------------
                # Happiness Score fuera de rango
                # --------------------------------------------

                if (
                    columna_score == columna
                    and not 0 <= valor <= 10
                ):

                    resultados.append({
                        "year": anio,
                        "country": pais,
                        "column": columna,
                        "value": valor,
                        "anomaly_type": "SCORE_OUT_OF_RANGE",
                        "detail": "Happiness Score debe estar entre 0 y 10.",
                    })

                # --------------------------------------------
                # Ranking inválido
                # --------------------------------------------

                if columna_rank == columna:

                    if valor < 1 or not float(valor).is_integer():

                        resultados.append({
                            "year": anio,
                            "country": pais,
                            "column": columna,
                            "value": valor,
                            "anomaly_type": "INVALID_RANK",
                            "detail": "El ranking debe ser un entero positivo.",
                        })

    return pd.DataFrame(resultados)


# ============================================================
# 14. DETECCIÓN ESTADÍSTICA DE OUTLIERS MEDIANTE IQR
# ============================================================

def detectar_outliers_iqr(datasets):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)

        for columna in obtener_registros_numericos(df):

            if columna in COLUMNAS_EXCLUIDAS_IQR:
                continue

            serie = pd.to_numeric(
                df[columna],
                errors="coerce",
            )

            serie = serie.replace(
                [np.inf, -np.inf],
                np.nan,
            ).dropna()

            if len(serie) < 4:
                continue

            q1 = serie.quantile(0.25)
            q3 = serie.quantile(0.75)

            iqr = q3 - q1

            if iqr == 0:
                continue

            limite_inferior = q1 - 1.5 * iqr
            limite_superior = q3 + 1.5 * iqr

            mascara = (
                (pd.to_numeric(
                    df[columna],
                    errors="coerce",
                ) < limite_inferior)
                |
                (pd.to_numeric(
                    df[columna],
                    errors="coerce",
                ) > limite_superior)
            )

            indices = df.index[mascara]

            for indice in indices:

                pais = (
                    df.loc[indice, columna_pais]
                    if columna_pais
                    else None
                )

                valor = df.loc[indice, columna]

                resultados.append({
                    "year": anio,
                    "country": pais,
                    "column": columna,
                    "value": valor,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "lower_bound": limite_inferior,
                    "upper_bound": limite_superior,
                    "anomaly_type": "IQR_OUTLIER",
                    "detail": (
                        "Valor fuera de los límites estadísticos "
                        "de 1.5 * IQR. Requiere revisión."
                    ),
                })

    return pd.DataFrame(resultados)


# ============================================================
# 15. RESUMEN DE CALIDAD POR DATASET
# ============================================================

def generar_resumen_calidad(
    datasets,
    duplicados_normalizados,
    empates_ranking,
    empates_score,
    anomalias_numericas,
):

    resultados = []

    for anio, df in datasets.items():

        columna_pais = obtener_nombre_pais(df)
        columna_rank = obtener_columna(df, "rank")
        columna_score = obtener_columna(df, "score")

        duplicados_pais_exactos = 0
        paises_unicos = 0

        if columna_pais:

            duplicados_pais_exactos = int(
                df[columna_pais].duplicated().sum()
            )

            paises_unicos = int(
                df[columna_pais].nunique()
            )

        duplicados_normalizados_anio = (
            duplicados_normalizados[
                duplicados_normalizados["year"] == anio
            ]
            if not duplicados_normalizados.empty
            else pd.DataFrame()
        )

        empates_rank_anio = (
            empates_ranking[
                empates_ranking["year"] == anio
            ]
            if not empates_ranking.empty
            else pd.DataFrame()
        )

        empates_score_anio = (
            empates_score[
                empates_score["year"] == anio
            ]
            if not empates_score.empty
            else pd.DataFrame()
        )

        anomalias_anio = (
            anomalias_numericas[
                (anomalias_numericas["year"] == anio)
                &
                (anomalias_numericas["anomaly_type"] == "INFINITE_VALUE")
            ]
            if not anomalias_numericas.empty
            else pd.DataFrame()
        )

        resultados.append({
            "year": anio,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "total_missing_cells": int(
                df.isna().sum().sum()
            ),
            "missing_pct": round(
                df.isna().sum().sum()
                / (df.shape[0] * df.shape[1]) * 100,
                2,
            ),
            "exact_duplicates": int(
                df.duplicated().sum()
            ),
            "duplicate_countries_exact": (
                duplicados_pais_exactos
            ),
            "duplicate_country_groups_normalized": (
                len(duplicados_normalizados_anio)
            ),
            "unique_countries": paises_unicos,
            "unique_rankings": (
                int(df[columna_rank].nunique())
                if columna_rank
                else None
            ),
            "ranking_tie_groups": len(empates_rank_anio),
            "unique_happiness_scores": (
                int(df[columna_score].nunique())
                if columna_score
                else None
            ),
            "score_tie_groups": len(empates_score_anio),
            "infinite_values": len(anomalias_anio),
        })

    return pd.DataFrame(resultados)


# ============================================================
# 16. EXPORTACIÓN DE REPORTES
# ============================================================

def exportar_reporte(df, nombre):

    ruta = CARPETA_REPORTES / nombre

    df.to_csv(
        ruta,
        index=False,
        encoding="utf-8-sig",
    )

    logger.info(
        "Reporte generado: %s",
        ruta,
    )


# ============================================================
# 17. EJECUCIÓN PRINCIPAL
# ============================================================

def main():

    CARPETA_REPORTES.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info("Iniciando perfilamiento de datasets.")

    datasets = cargar_datasets()

    # --------------------------------------------
    # Reportes generales
    # --------------------------------------------

    inventario = generar_inventario(datasets)

    perfil_columnas = generar_perfil_columnas(
        datasets
    )

    comparacion_esquemas = generar_comparacion_esquemas(
        datasets
    )

    detalle_faltantes = generar_detalle_faltantes(
        datasets
    )

    # --------------------------------------------
    # Validaciones adicionales
    # --------------------------------------------

    duplicados_normalizados = validar_duplicados_paises(
        datasets
    )

    empates_ranking = validar_empates_ranking(
        datasets
    )

    empates_score = validar_empates_score(
        datasets
    )

    cobertura_paises = generar_cobertura_paises(
        datasets
    )

    conflictos_region = validar_conflictos_region(
        datasets
    )

    anomalias_numericas = validar_anomalias_numericas(
        datasets
    )

    outliers_iqr = detectar_outliers_iqr(
        datasets
    )

    # --------------------------------------------
    # Resumen de calidad
    # --------------------------------------------

    resumen_calidad = generar_resumen_calidad(
        datasets,
        duplicados_normalizados,
        empates_ranking,
        empates_score,
        anomalias_numericas,
    )

    # --------------------------------------------
    # Exportación
    # --------------------------------------------

    reportes = {
        "dataset_inventory.csv": inventario,
        "column_profile.csv": perfil_columnas,
        "schema_comparison.csv": comparacion_esquemas,
        "quality_summary.csv": resumen_calidad,
        "missing_values_detail.csv": detalle_faltantes,
        "normalized_country_duplicates.csv": duplicados_normalizados,
        "ranking_ties.csv": empates_ranking,
        "happiness_score_ties.csv": empates_score,
        "country_coverage.csv": cobertura_paises,
        "region_conflicts.csv": conflictos_region,
        "numeric_anomalies.csv": anomalias_numericas,
        "iqr_outliers.csv": outliers_iqr,
    }

    for nombre, reporte in reportes.items():
        exportar_reporte(reporte, nombre)

    # --------------------------------------------
    # Resumen en consola
    # --------------------------------------------

    logger.info("=" * 60)
    logger.info("PERFILAMIENTO FINALIZADO")
    logger.info("=" * 60)

    logger.info(
        "Total de registros analizados: %s",
        inventario["rows"].sum(),
    )

    logger.info(
        "Total de valores faltantes: %s",
        resumen_calidad["total_missing_cells"].sum(),
    )

    logger.info(
        "Grupos de países duplicados normalizados: %s",
        len(duplicados_normalizados),
    )

    logger.info(
        "Grupos con empates en ranking: %s",
        len(empates_ranking),
    )

    logger.info(
        "Grupos con empates en Happiness Score: %s",
        len(empates_score),
    )

    logger.info(
        "Países identificados entre todos los años: %s",
        len(cobertura_paises),
    )

    logger.info(
        "Conflictos de región: %s",
        len(conflictos_region),
    )

    logger.info(
        "Anomalías numéricas: %s",
        len(anomalias_numericas),
    )

    logger.info(
        "Posibles outliers IQR: %s",
        len(outliers_iqr),
    )

    logger.info(
        "Reportes guardados en: %s",
        CARPETA_REPORTES,
    )


if __name__ == "__main__":
    main()
