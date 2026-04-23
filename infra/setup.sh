#!/usr/bin/env bash
# =============================================================================
# Resparked — GCP Data Intelligence Infrastructure
# =============================================================================
#
# Script de despliegue de infraestructura base para el pipeline de datos
# de la empresa de e-commerce Resparked.
#
# Uso:
#   bash infra/setup.sh
#
# Prerequisitos:
#   - gcloud CLI instalado y autenticado (gcloud auth login)
#   - Proyecto de GCP creado y configurado (gcloud config set project <ID>)
#
# El script es IDEMPOTENTE: puede ejecutarse múltiples veces sin efectos
# secundarios. Cada recurso se verifica antes de crearse.
# =============================================================================

set -euo pipefail

# =============================================================================
# VARIABLES PARAMETRIZABLES
# =============================================================================
# Modificá estos valores según tu entorno. PROJECT_ID se toma del proyecto
# activo de gcloud si no se especifica.

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
DATASET_NAME="${DATASET_NAME:-dropshipping}"

# Nombres derivados de los buckets
BUCKET_RAW="${PROJECT_ID}-raw-products"
BUCKET_FAILED="${PROJECT_ID}-failed-products"

# Nombre del tópico de Pub/Sub para monitoreo de precios
PUBSUB_TOPIC="price-updates"

# Directorio temporal para archivos de schema (relativo al script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_DIR="${SCRIPT_DIR}/schemas_tmp"

# =============================================================================
# UTILIDADES
# =============================================================================

# Colores para output legible
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}   $1"; }
log_warn()    { echo -e "${YELLOW}[SKIP]${NC} $1"; }
log_error()   { echo -e "${RED}[ERR]${NC}  $1"; }
log_section() { echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# =============================================================================
# VALIDACIÓN INICIAL
# =============================================================================

log_section "Validación del entorno"

# Verificar que gcloud esté instalado
if ! command -v gcloud &>/dev/null; then
    log_error "gcloud CLI no está instalado. Instalalo desde: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Verificar que haya un proyecto configurado
if [[ -z "${PROJECT_ID}" ]]; then
    log_error "No se detectó un PROJECT_ID. Configuralo con:"
    log_error "  export PROJECT_ID=tu-proyecto"
    log_error "  o: gcloud config set project tu-proyecto"
    exit 1
fi

log_info "Proyecto:  ${PROJECT_ID}"
log_info "Región:    ${REGION}"
log_info "Dataset:   ${DATASET_NAME}"
log_info "Bucket Raw:    gs://${BUCKET_RAW}"
log_info "Bucket Failed: gs://${BUCKET_FAILED}"
log_info "Pub/Sub Topic: ${PUBSUB_TOPIC}"

# =============================================================================
# PASO 1: HABILITACIÓN DE APIs
# =============================================================================
# Habilitamos las APIs necesarias para el pipeline:
#   - storage:        Google Cloud Storage (buckets para CSVs)
#   - bigquery:       BigQuery (warehouse analítico)
#   - cloudfunctions: Cloud Functions (procesamiento serverless)
#   - pubsub:         Pub/Sub (mensajería para precios en tiempo real)
#   - eventarc:       Eventarc (triggers basados en eventos para Functions)
#   - logging:        Cloud Logging (observabilidad del pipeline)

log_section "Paso 1: Habilitación de APIs"

APIS=(
    "storage.googleapis.com"
    "bigquery.googleapis.com"
    "cloudfunctions.googleapis.com"
    "pubsub.googleapis.com"
    "eventarc.googleapis.com"
    "logging.googleapis.com"
)

for api in "${APIS[@]}"; do
    log_info "Habilitando ${api}..."
    gcloud services enable "${api}" --project="${PROJECT_ID}" || true
done

# Esperamos 20 segundos para que las APIs se propaguen completamente.
# Esto es necesario porque algunos servicios (como BigQuery o Eventarc)
# requieren un tiempo de activación interno antes de poder usarse.
log_info "Esperando 20 segundos para propagación de APIs..."
sleep 20
log_success "APIs habilitadas y propagadas"

# =============================================================================
# PASO 2: GOOGLE CLOUD STORAGE — BUCKETS
# =============================================================================
# Creamos dos buckets con acceso uniforme a nivel de bucket (IAM):
#
#   1. raw-products:    Recibe los CSV exportados desde Shopify.
#                       La Cloud Function de validación leerá de acá.
#
#   2. failed-products: Almacena los archivos que no pasaron la validación.
#                       Permite auditoría y reprocesamiento manual.
#
# --uniform-bucket-level-access deshabilita las ACLs por objeto y fuerza
# el uso exclusivo de IAM, que es la práctica recomendada de seguridad.

log_section "Paso 2: Google Cloud Storage"

create_bucket() {
    local bucket_name="$1"
    local description="$2"

    if gsutil ls -b "gs://${bucket_name}" &>/dev/null; then
        log_warn "Bucket gs://${bucket_name} ya existe (${description})"
    else
        log_info "Creando bucket gs://${bucket_name} (${description})..."
        gsutil mb \
            -p "${PROJECT_ID}" \
            -l "${REGION}" \
            -b on \
            "gs://${bucket_name}"
        log_success "Bucket gs://${bucket_name} creado"
    fi
}

create_bucket "${BUCKET_RAW}"    "CSV de proveedores Shopify"
create_bucket "${BUCKET_FAILED}" "Archivos descartados por validación"

# =============================================================================
# PASO 3: BIGQUERY — DATASET Y TABLAS
# =============================================================================
# Creamos el dataset y las tablas necesarias para el pipeline:
#
#   Dataset: dropshipping (configurable)
#     └── staging_products   → Datos crudos tras la ingesta inicial
#     └── final_products     → Datos validados y listos para análisis
#     └── enriched_products  → Datos enriquecidos con categorías de Gemini AI
#
# NOTA: Usamos archivos JSON temporales para definir los schemas en lugar
# de pasarlos inline al CLI. Esto evita problemas de escape de caracteres
# (comillas, corchetes, etc.) que son comunes en shells de distintos OS.

log_section "Paso 3: BigQuery — Dataset y Tablas"

# ---- 3.1: Crear dataset ----
if bq show --project_id="${PROJECT_ID}" "${DATASET_NAME}" &>/dev/null; then
    log_warn "Dataset ${DATASET_NAME} ya existe"
else
    log_info "Creando dataset ${DATASET_NAME}..."
    bq mk \
        --dataset \
        --location="${REGION}" \
        --project_id="${PROJECT_ID}" \
        "${DATASET_NAME}"
    log_success "Dataset ${DATASET_NAME} creado"
fi

# ---- 3.2: Generar archivos de schema temporales ----
# Creamos un directorio temporal para los schemas JSON.
# Estos archivos son efímeros: se generan, se usan, y se borran al final.
mkdir -p "${SCHEMA_DIR}"

# Schema compartido para staging_products y final_products.
# Representa un producto tal como viene del proveedor de Shopify,
# con un timestamp de ingesta para trazabilidad.
cat > "${SCHEMA_DIR}/products_schema.json" <<'SCHEMA_PRODUCTS'
[
    {"name": "product_id",   "type": "STRING",    "mode": "REQUIRED",  "description": "Identificador único del producto en Shopify"},
    {"name": "name",         "type": "STRING",    "mode": "NULLABLE",  "description": "Nombre del producto"},
    {"name": "description",  "type": "STRING",    "mode": "NULLABLE",  "description": "Descripción del producto"},
    {"name": "price",        "type": "FLOAT",     "mode": "NULLABLE",  "description": "Precio unitario en USD"},
    {"name": "supplier",     "type": "STRING",    "mode": "NULLABLE",  "description": "Nombre del proveedor/supplier"},
    {"name": "stock",        "type": "INTEGER",   "mode": "NULLABLE",  "description": "Cantidad en stock disponible"},
    {"name": "ingested_at",  "type": "TIMESTAMP", "mode": "NULLABLE",  "description": "Fecha y hora de ingesta al pipeline"}
]
SCHEMA_PRODUCTS

# Schema para enriched_products.
# Contiene los campos generados por Gemini AI para categorización.
# NOTA: El campo 'tags' usa mode REPEATED (ARRAY de strings) para
# almacenar múltiples etiquetas por producto sin desnormalizar.
cat > "${SCHEMA_DIR}/enriched_schema.json" <<'SCHEMA_ENRICHED'
[
    {"name": "product_id",       "type": "STRING",    "mode": "REQUIRED",  "description": "FK al producto original en final_products"},
    {"name": "category",         "type": "STRING",    "mode": "NULLABLE",  "description": "Categoría principal asignada por Gemini AI"},
    {"name": "subcategory",      "type": "STRING",    "mode": "NULLABLE",  "description": "Subcategoría asignada por Gemini AI"},
    {"name": "tags",             "type": "STRING",    "mode": "REPEATED",  "description": "Etiquetas generadas por AI (ARRAY de strings)"},
    {"name": "confidence_score", "type": "FLOAT",     "mode": "NULLABLE",  "description": "Score de confianza de la clasificación AI (0.0-1.0)"},
    {"name": "enriched_at",      "type": "TIMESTAMP", "mode": "NULLABLE",  "description": "Fecha y hora del enriquecimiento"}
]
SCHEMA_ENRICHED

log_success "Archivos de schema generados en ${SCHEMA_DIR}"

# ---- 3.3: Crear tablas ----
# Función auxiliar para crear una tabla BigQuery de forma idempotente.
# Si la tabla ya existe, la saltea. Si no, la crea con el schema JSON indicado.
create_bq_table() {
    local table_name="$1"
    local schema_file="$2"
    local description="$3"
    local full_table="${PROJECT_ID}:${DATASET_NAME}.${table_name}"

    if bq show "${full_table}" &>/dev/null; then
        log_warn "Tabla ${DATASET_NAME}.${table_name} ya existe"
    else
        log_info "Creando tabla ${DATASET_NAME}.${table_name}..."
        bq mk \
            --table \
            --project_id="${PROJECT_ID}" \
            --description="${description}" \
            "${DATASET_NAME}.${table_name}" \
            "${schema_file}"
        log_success "Tabla ${DATASET_NAME}.${table_name} creada"
    fi
}

# staging_products: Primera parada de los datos crudos del CSV.
# Los datos llegan acá después de pasar la validación básica de la Cloud Function.
create_bq_table \
    "staging_products" \
    "${SCHEMA_DIR}/products_schema.json" \
    "Productos en staging — datos crudos post-validación"

# final_products: Datos limpios y listos para consumo analítico.
# Se puebla desde staging_products después de transformaciones de calidad.
create_bq_table \
    "final_products" \
    "${SCHEMA_DIR}/products_schema.json" \
    "Productos finales — datos validados y transformados"

# enriched_products: Datos enriquecidos con categorización de Gemini AI.
# Contiene la clasificación automática y el confidence score.
create_bq_table \
    "enriched_products" \
    "${SCHEMA_DIR}/enriched_schema.json" \
    "Productos enriquecidos — categorización AI con Gemini"

# ---- 3.4: Limpiar schemas temporales ----
# Borramos los archivos JSON temporales que ya cumplieron su propósito.
# No queremos que queden en el repo (además, el .gitignore ya excluye *.json).
rm -rf "${SCHEMA_DIR}"
log_info "Archivos de schema temporales eliminados"

# =============================================================================
# PASO 4: PUB/SUB — TÓPICO DE MONITOREO DE PRECIOS
# =============================================================================
# Creamos el tópico para el pipeline de monitoreo de precios en tiempo real.
# El simulador de precios (módulo 03) publicará actualizaciones acá,
# y una Cloud Function suscrita las ingresará a BigQuery para análisis
# de series temporales.

log_section "Paso 4: Pub/Sub — Tópico de Precios"

if gcloud pubsub topics describe "${PUBSUB_TOPIC}" --project="${PROJECT_ID}" &>/dev/null; then
    log_warn "Tópico ${PUBSUB_TOPIC} ya existe"
else
    log_info "Creando tópico ${PUBSUB_TOPIC}..."
    gcloud pubsub topics create "${PUBSUB_TOPIC}" --project="${PROJECT_ID}"
    log_success "Tópico ${PUBSUB_TOPIC} creado"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================

log_section "✅ Infraestructura desplegada exitosamente"

echo ""
log_info "Recursos creados:"
echo "  📦 Buckets:"
echo "     └── gs://${BUCKET_RAW}"
echo "     └── gs://${BUCKET_FAILED}"
echo "  📊 BigQuery:"
echo "     └── ${DATASET_NAME}.staging_products"
echo "     └── ${DATASET_NAME}.final_products"
echo "     └── ${DATASET_NAME}.enriched_products"
echo "  📨 Pub/Sub:"
echo "     └── ${PUBSUB_TOPIC}"
echo ""
log_info "Próximo paso: Desplegá la Cloud Function de validación (módulo 01)"
echo ""
