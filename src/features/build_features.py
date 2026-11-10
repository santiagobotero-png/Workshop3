# ============================================================
# 1. IMPORTACIONES Y CONFIGURACIÓN
# ============================================================
from sklearn.linear_model import LinearRegression
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

# ============================================================
# 2. RUTAS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "world_happiness_2015_2019.csv"
)


# ============================================================
# 3. DEFINICIÓN DE FEATURES Y TARGET
# ============================================================

FEATURE_COLUMNS = [
    "gdp",
    "social_support",
    "health",
    "freedom",
    "corruption",
    "generosity"
]

TARGET_COLUMN = "happiness_score"

EXCLUDED_COLUMNS = [
    "year",
    "country",
    "happiness_rank"
]


# ============================================================
# 4. CARGA DEL DATASET
# ============================================================

df = pd.read_csv(INPUT_FILE)

print("=" * 65)
print("SELECCIÓN DE VARIABLES PARA MACHINE LEARNING")
print("=" * 65)

print(f"\nRegistros cargados: {len(df)}")


# ============================================================
# 5. VALIDACIÓN DE COLUMNAS
# ============================================================

required_columns = FEATURE_COLUMNS + [TARGET_COLUMN]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Faltan columnas necesarias: {missing_columns}"
    )

print("\n[OK] Todas las variables requeridas están disponibles.")


# ============================================================
# 6. CONSTRUCCIÓN DE FEATURES Y TARGET
# ============================================================

X = df[FEATURE_COLUMNS].copy()

y = df[TARGET_COLUMN].copy()


# ============================================================
# 7. VALIDACIÓN DE TIPOS DE DATOS
# ============================================================

if not all(
    pd.api.types.is_numeric_dtype(X[column])
    for column in FEATURE_COLUMNS
):
    raise TypeError(
        "Todas las features deben ser numéricas."
    )

if not pd.api.types.is_numeric_dtype(y):
    raise TypeError(
        "El target debe ser numérico."
    )


# ============================================================
# 8. RESUMEN DE LA SELECCIÓN
# ============================================================

print("\nFEATURES SELECCIONADAS:")
for column in FEATURE_COLUMNS:
    print(f"  - {column}")

print(f"\nTARGET: {TARGET_COLUMN}")

print("\nVARIABLES EXCLUIDAS:")
for column in EXCLUDED_COLUMNS:
    print(f"  - {column}")

print("\nDIMENSIONES:")
print(f"X: {X.shape}")
print(f"y: {y.shape}")

print("\nTIPOS DE DATOS:")
print(X.dtypes.to_string())

print("\nVALORES FALTANTES EN FEATURES:")
print(X.isna().sum().to_string())

print("\nVALORES FALTANTES EN TARGET:")
print(y.isna().sum())

print("\n" + "=" * 65)
print("SELECCIÓN DE VARIABLES COMPLETADA")
print("=" * 65)


# ============================================================
# 9. ESTADÍSTICAS DESCRIPTIVAS
# ============================================================

print("\n" + "=" * 65)
print("ESTADÍSTICAS DESCRIPTIVAS DE LAS FEATURES")
print("=" * 65)

print(
    X.describe()
    .round(3)
    .to_string()
)

print("\nESTADÍSTICAS DEL TARGET:")

print(
    y.describe()
    .round(3)
    .to_string()
)


# ============================================================
# 10. CORRELACIÓN ENTRE FEATURES Y TARGET
# ============================================================

print("\n" + "=" * 65)
print("CORRELACIÓN DE FEATURES CON HAPPINESS SCORE")
print("=" * 65)

df_correlation = X.copy()
df_correlation[TARGET_COLUMN] = y

correlation_target = (
    df_correlation.corr(numeric_only=True)[TARGET_COLUMN]
    .drop(TARGET_COLUMN)
    .sort_values(ascending=False)
)

print(
    correlation_target.round(3).to_string()
)



# ============================================================
# 11. CORRELACIÓN ENTRE FEATURES
# ============================================================

print("\n" + "=" * 65)
print("MATRIZ DE CORRELACIÓN ENTRE FEATURES")
print("=" * 65)

correlation_features = X.corr()

print(
    correlation_features.round(3).to_string()
)

# ============================================================
# 12. VISUALIZACIÓN DE CORRELACIONES
# ============================================================

plt.figure(figsize=(10, 7))

sns.heatmap(
    df_correlation.corr(numeric_only=True),
    annot=True,
    cmap="coolwarm",
    fmt=".2f",
    center=0
)

plt.title("Correlación entre Features y Happiness Score")
plt.tight_layout()
plt.show()


# ============================================================
# 13. RELACIÓN ENTRE FEATURES Y TARGET
# ============================================================

fig, axes = plt.subplots(2, 3, figsize=(16, 9))

for ax, feature in zip(axes.flatten(), FEATURE_COLUMNS):

    sns.regplot(
        data=df,
        x=feature,
        y=TARGET_COLUMN,
        ax=ax,
        scatter_kws={"alpha": 0.35},
        line_kws={"color": "red"}
    )

    ax.set_title(f"{feature} vs Happiness Score")

plt.tight_layout()
plt.show()


# ============================================================
# 14. PREPARACIÓN INICIAL DE FEATURES
# ============================================================

print("\n" + "=" * 65)
print("PREPARACIÓN INICIAL DE FEATURES")
print("=" * 65)

# Redondear a tres decimales
X = X.round(3)
y = y.round(3)

print("\nPrecisión decimal estandarizada: 3 decimales")

print("\nValores faltantes antes del preprocesamiento:")
print(X.isna().sum().to_string())

print("\nTarget:")
print(f"Valores faltantes: {y.isna().sum()}")

print("\n[OK] Preparación inicial completada.")

# ============================================================
# 15. SEPARACIÓN TRAIN / TEST
# ============================================================

from sklearn.model_selection import train_test_split


print("\n" + "=" * 65)
print("SEPARACIÓN DE DATOS: TRAIN / TEST")
print("=" * 65)


# Separación 80/20
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    shuffle=True
)


# Dimensiones
print("\nDIMENSIONES DE LOS CONJUNTOS:")

print(f"X_train: {X_train.shape}")
print(f"X_test:  {X_test.shape}")

print(f"y_train: {y_train.shape}")
print(f"y_test:  {y_test.shape}")


# Proporciones
print("\nDISTRIBUCIÓN:")

print(
    f"Entrenamiento: "
    f"{len(X_train)} registros "
    f"({len(X_train) / len(X) * 100:.2f}%)"
)

print(
    f"Prueba: "
    f"{len(X_test)} registros "
    f"({len(X_test) / len(X) * 100:.2f}%)"
)


# Valores faltantes
print("\nVALORES FALTANTES EN TRAIN:")

print(X_train.isna().sum().to_string())

print("\nVALORES FALTANTES EN TEST:")

print(X_test.isna().sum().to_string())


print("\n" + "=" * 65)
print("SEPARACIÓN COMPLETADA")
print("=" * 65)

# ============================================================
# 16. CONSTRUCCIÓN DEL PREPROCESADOR
# ============================================================

print("\n" + "=" * 65)
print("CONSTRUCCIÓN DEL PREPROCESADOR")
print("=" * 65)


numeric_preprocessor = Pipeline(
    steps=[
        (
            "imputer",
            SimpleImputer(strategy="median")
        ),
        (
            "scaler",
            StandardScaler()
        )
    ]
)


# Inspección de componentes
print("\nCOMPONENTES DEL PREPROCESADOR:")

for name, transformer in numeric_preprocessor.steps:
    print(f"  - {name}: {transformer.__class__.__name__}")


print("\nESTRATEGIA DE IMPUTACIÓN: mediana")
print("MÉTODO DE ESCALAMIENTO: StandardScaler")

print("\nFEATURES DE ENTRADA:")
for feature in FEATURE_COLUMNS:
    print(f"  - {feature}")

print("\n[OK] Preprocesador construido correctamente.")

print("\n" + "=" * 65)
print("PREPROCESADOR LISTO")
print("=" * 65)


# ============================================================
# 17. CONSTRUCCIÓN DEL PIPELINE COMPLETO
# ============================================================

print("\n" + "=" * 65)
print("CONSTRUCCIÓN DEL PIPELINE DE MACHINE LEARNING")
print("=" * 65)


model_pipeline = Pipeline(
    steps=[
        (
            "preprocessor",
            numeric_preprocessor
        ),
        (
            "regressor",
            LinearRegression()
        )
    ]
)


# Inspección del pipeline
print("\nESTRUCTURA DEL PIPELINE:")

for name, component in model_pipeline.steps:
    print(f"  - {name}: {component.__class__.__name__}")


print("\nMODELO: LinearRegression")
print("PREPROCESAMIENTO: Imputación por mediana + StandardScaler")

print("\nFEATURES:")
for feature in FEATURE_COLUMNS:
    print(f"  - {feature}")

print(f"\nTARGET: {TARGET_COLUMN}")

print("\n[OK] Pipeline construido correctamente.")

print("\n" + "=" * 65)
print("PIPELINE LISTO PARA ENTRENAMIENTO")
print("=" * 65)


# ============================================================
# 18. ENTRENAMIENTO DEL MODELO
# ============================================================

print("\n" + "=" * 65)
print("ENTRENAMIENTO DEL MODELO")
print("=" * 65)


# Ajustar el pipeline exclusivamente con entrenamiento
model_pipeline.fit(X_train, y_train)


print("\n[OK] Modelo entrenado correctamente.")

print("\nINFORMACIÓN DEL ENTRENAMIENTO:")
print(f"Registros utilizados: {len(X_train)}")
print(f"Features utilizadas: {len(FEATURE_COLUMNS)}")
print(f"Target: {TARGET_COLUMN}")

print("\nCOMPONENTES AJUSTADOS:")

print(
    "Imputador:",
    model_pipeline.named_steps["preprocessor"]
    .named_steps["imputer"].__class__.__name__
)

print(
    "Escalador:",
    model_pipeline.named_steps["preprocessor"]
    .named_steps["scaler"].__class__.__name__
)

print(
    "Regresor:",
    model_pipeline.named_steps["regressor"].__class__.__name__
)


print("\n" + "=" * 65)
print("ENTRENAMIENTO COMPLETADO")
print("=" * 65)

# ============================================================
# 19. INSPECCIÓN DE PARÁMETROS APRENDIDOS
# ============================================================

print("\n" + "=" * 65)
print("INSPECCIÓN DE PARÁMETROS APRENDIDOS")
print("=" * 65)

# Recuperar componentes ajustados
imputer = (
    model_pipeline.named_steps["preprocessor"]
    .named_steps["imputer"]
)

scaler = (
    model_pipeline.named_steps["preprocessor"]
    .named_steps["scaler"]
)

regressor = model_pipeline.named_steps["regressor"]

# Valores utilizados para imputar datos faltantes
print("\nMEDIANAS APRENDIDAS POR EL IMPUTADOR:")

for feature, median in zip(FEATURE_COLUMNS, imputer.statistics_):
    print(f"{feature}: {median:.3f}")

# Parámetros de estandarización
print("\nPARÁMETROS APRENDIDOS POR EL ESCALADOR:")

for feature, mean, scale in zip(
    FEATURE_COLUMNS,
    scaler.mean_,
    scaler.scale_
):
    print(f"{feature}: media={mean:.3f}, escala={scale:.3f}")

# Coeficientes de regresión
print("\nCOEFICIENTES DEL MODELO:")

for feature, coefficient in zip(
    FEATURE_COLUMNS,
    regressor.coef_
):
    print(f"{feature}: {coefficient:.3f}")

print(f"\nIntercepto: {regressor.intercept_:.3f}")

print("\n" + "=" * 65)
print("INSPECCIÓN COMPLETADA")
print("=" * 65)


# ============================================================
# 20. EVALUACIÓN DEL MODELO
# ============================================================

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)

print("\n" + "=" * 65)
print("EVALUACIÓN DEL MODELO")
print("=" * 65)

# Generar predicciones sobre el conjunto de prueba
y_pred = model_pipeline.predict(X_test)

# Calcular métricas
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = mse ** 0.5
r2 = r2_score(y_test, y_pred)

print("\nMÉTRICAS DE EVALUACIÓN:")

print(f"MAE  : {mae:.3f}")
print(f"MSE  : {mse:.3f}")
print(f"RMSE : {rmse:.3f}")
print(f"R²   : {r2:.3f}")

# Comparación entre valores reales y predichos
df_predictions = pd.DataFrame({
    "real": y_test.values,
    "predicho": y_pred
})

df_predictions["error"] = (
    df_predictions["real"] - df_predictions["predicho"]
)

print("\nCOMPARACIÓN DE PREDICCIONES:")
print(df_predictions.head(10).round(3).to_string(index=False))

print("\nRESUMEN DE ERRORES:")
print(df_predictions["error"].describe().round(3))

print("\n" + "=" * 65)
print("EVALUACIÓN COMPLETADA")
print("=" * 65)

# ============================================================
# 21. DIAGNÓSTICO VISUAL DEL MODELO
# ============================================================

# 1. Valores reales vs. predichos
plt.figure(figsize=(8, 6))

plt.scatter(
    df_predictions["real"],
    df_predictions["predicho"],
    alpha=0.6
)

min_value = min(
    df_predictions["real"].min(),
    df_predictions["predicho"].min()
)

max_value = max(
    df_predictions["real"].max(),
    df_predictions["predicho"].max()
)

plt.plot(
    [min_value, max_value],
    [min_value, max_value],
    linestyle="--",
    color="red",
    label="Predicción perfecta"
)

plt.xlabel("Happiness Score real")
plt.ylabel("Happiness Score predicho")
plt.title("Valores reales vs. predichos")
plt.legend()
plt.tight_layout()
plt.show()


# 2. Distribución de errores
plt.figure(figsize=(8, 5))

sns.histplot(
    df_predictions["error"],
    bins=25,
    kde=True
)

plt.axvline(0, color="red", linestyle="--")

plt.xlabel("Error de predicción")
plt.ylabel("Frecuencia")
plt.title("Distribución de errores del modelo")
plt.tight_layout()
plt.show()


# 3. Errores vs. predicciones
plt.figure(figsize=(8, 5))

plt.scatter(
    df_predictions["predicho"],
    df_predictions["error"],
    alpha=0.6
)

plt.axhline(0, color="red", linestyle="--")

plt.xlabel("Happiness Score predicho")
plt.ylabel("Error de predicción")
plt.title("Errores vs. valores predichos")
plt.tight_layout()
plt.show()


# ============================================================
# 21. SERIALIZACIÓN DEL PIPELINE
# ============================================================

MODEL_DIR = PROJECT_ROOT / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = MODEL_DIR / "happiness_regression_pipeline.joblib"

# Guardar pipeline completo
joblib.dump(model_pipeline, MODEL_FILE)

print("\n" + "=" * 65)
print("SERIALIZACIÓN DEL MODELO")
print("=" * 65)

print("\n[OK] Pipeline guardado correctamente.")
print(f"Ubicación: {MODEL_FILE}")
print(f"Tamaño: {MODEL_FILE.stat().st_size / 1024:.2f} KB")

print("\nCOMPONENTES SERIALIZADOS:")
print("- SimpleImputer")
print("- StandardScaler")
print("- LinearRegression")

print("\n" + "=" * 65)
print("SERIALIZACIÓN COMPLETADA")
print("=" * 65)


# ============================================================
# 22. VALIDACIÓN DE RECUPERACIÓN DEL PIPELINE
# ============================================================

print("\n" + "=" * 65)
print("VALIDACIÓN DE RECUPERACIÓN DEL PIPELINE")
print("=" * 65)

# Recuperar pipeline serializado
loaded_pipeline = joblib.load(MODEL_FILE)

print("\n[OK] Pipeline recuperado correctamente.")

# Generar predicciones con el pipeline recuperado
y_pred_loaded = loaded_pipeline.predict(X_test)

# Comparar predicciones originales y recuperadas
predictions_match = (
    y_pred == y_pred_loaded
).all()

max_difference = abs(
    y_pred - y_pred_loaded
).max()

print("\nVALIDACIÓN DE PREDICCIONES:")
print(f"Predicciones idénticas: {predictions_match}")
print(f"Diferencia máxima: {max_difference:.12f}")

# Comparar métricas
loaded_mae = mean_absolute_error(y_test, y_pred_loaded)
loaded_rmse = mean_squared_error(y_test, y_pred_loaded) ** 0.5
loaded_r2 = r2_score(y_test, y_pred_loaded)

print("\nMÉTRICAS DEL PIPELINE RECUPERADO:")
print(f"MAE  : {loaded_mae:.3f}")
print(f"RMSE : {loaded_rmse:.3f}")
print(f"R²   : {loaded_r2:.3f}")

print("\n" + "=" * 65)
print("VALIDACIÓN COMPLETADA")
print("=" * 65)