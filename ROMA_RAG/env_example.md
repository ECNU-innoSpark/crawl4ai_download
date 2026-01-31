# ROMA-DSPy Environment Configuration
# Copy this file to .env and fill in your actual values

# ==============================================================================
# Storage Configuration (S3 + goofys)
# ==============================================================================

# Base storage path - mounted S3 bucket location (must be same on host and E2B)
# Default: /opt/senient
STORAGE_BASE_PATH=./storage

# S3 bucket name for persistent storage (existing bucket)
ROMA_S3_BUCKET=roma-shared

# AWS region for S3 bucket
AWS_REGION=us-east-1

# ==============================================================================
# AWS Credentials
# ==============================================================================

# AWS Access Key ID
# - For S3 access via goofys: use your AWS credentials
# - For local MinIO (MLflow artifacts): set to MINIO_ROOT_USER (default: minioadmin)
AWS_ACCESS_KEY_ID=minioadmin

# AWS Secret Access Key
# - For S3 access via goofys: use your AWS credentials
# - For local MinIO (MLflow artifacts): set to MINIO_ROOT_PASSWORD (default: minioadmin123)
AWS_SECRET_ACCESS_KEY=minioadmin123

# ==============================================================================
# LLM API Keys
# ==============================================================================

# OpenAI API Key (for GPT models)
OPENAI_API_KEY=sk-5X4gFsZhzWOplXNSaaRie5Rpz0s1spvuTDaNWqLTaOWyHbrL
OPENAI_BASE_URL=http://49.51.37.239:3008/v1

# Anthropic API Key (only if NOT using OpenRouter)
ANTHROPIC_API_KEY=sk-MHomU5EVfPyKlfWhMlXWp4CjYOgdJYAfVjOK21EUaPsRUqK5
ANTHROPIC_BASE_URL=https://www.packyapi.com

# Google GenAI API Key (only if NOT using OpenRouter)
GOOGLE_API_KEY=sk-ke2ZpDtZkhNIf1STOXuTZ6F5tx8P8YQJTAa3Uk5vDCakaHeN
GOOGLE_BASE_URL=https://www.packyapi.com/v1

# Fireworks AI API Key (optional)
FIREWORKS_API_KEY=your_fireworks_api_key

# OpenRouter API Key (RECOMMENDED - single key for all models)
OPENROUTER_API_KEY=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
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1

# Serper API Key (for web search functionality)
SERPER_API_KEY=674fabae79982e3e941316671a42246771322549

# ==============================================================================
# MCP Server API Keys
# ==============================================================================
# Add API keys for any MCP (Model Context Protocol) servers you are using.
# These are passed to MCP servers configured in your profiles (config/profiles/*.yaml)
# Common MCP servers that require API keys:

# Exa API Key (for web search via MCP)
EXA_API_KEY=d7b640f6-dc9c-417e-a243-ed1a2e3ce9a6

# Brave Search API Key (for web search via MCP)
# BRAVE_API_KEY=your_brave_api_key

# Add any additional MCP server API keys here as needed
# Example: YOUR_MCP_SERVICE_API_KEY=your_key_here

# ==============================================================================
# E2B Configuration
# ==============================================================================

# E2B API Key (for code execution sandboxes)
E2B_API_KEY=e2b_d7559fabe393789af1e5d8bd088fe828838bc87b

# E2B Template ID (E2B v2 template with runtime S3 mounting)
# Build templates: just e2b-build (dev) or just e2b-build-prod (production)
# Development template (default): roma-dspy-sandbox-dev
# Production template: roma-dspy-sandbox
# Default: "roma-dspy-sandbox-dev" if not set
# For production, set: E2B_TEMPLATE_ID=roma-dspy-sandbox
E2B_TEMPLATE_ID=roma-sandbox-sc

# E2B sandbox timeout in seconds (default: 300 = 5 minutes)
E2B_TIMEOUT=9000

# ==============================================================================
# Crypto/Finance API Keys (for toolkit usage)
# ==============================================================================

# DefiLlama API Key (optional, for Pro features)
DEFILLAMA_API_KEY=your_defillama_api_key

# Arkham Intelligence API Key (for on-chain analytics)
ARKHAM_API_KEY=your_arkham_api_key

# Binance API credentials (optional, for trading features)
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret

# CoinGecko API Key (optional, for Pro API)
COINGECKO_API_KEY=your_coingecko_api_key

# Coinglass API Key (for derivatives market data)
COINGLASS_API_KEY=your_coinglass_api_key

# ==============================================================================
# Database Configuration (PostgreSQL)
# ==============================================================================

# PostgreSQL connection (for checkpoints and execution tracking)
POSTGRES_ENABLED=true
POSTGRES_DB=roma_dspy
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# Full database URL (auto-constructed if not set)
# DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/roma_dspy

# PostgreSQL pool settings
POSTGRES_POOL_SIZE=5
POSTGRES_MAX_OVERFLOW=10
POSTGRES_POOL_TIMEOUT=30.0
SQL_ECHO=false

# ==============================================================================
# API Server Configuration
# ==============================================================================

# API server host and port
API_HOST=0.0.0.0
API_PORT=8000

# Number of API workers (for production)
API_WORKERS=4

# ==============================================================================
# ROMA-DSPy Runtime Configuration
# ==============================================================================

# Environment (development, testing, production)
ROMA_ENV=development

# Maximum decomposition depth
ROMA_MAX_DEPTH=2

# Verbose logging
ROMA_VERBOSE=false

# Enable debug logging
ROMA_ENABLE_LOGGING=true

# ==============================================================================
# Resilience Configuration
# ==============================================================================

# Retry settings
ROMA_RETRY_ENABLED=true
ROMA_MAX_RETRIES=3
ROMA_RETRY_STRATEGY=exponential_backoff

# Circuit breaker settings
ROMA_CIRCUIT_BREAKER_ENABLED=true
ROMA_FAILURE_THRESHOLD=5

# ==============================================================================
# Cache Configuration
# ==============================================================================

# Enable DSPy caching
ROMA_CACHE_ENABLED=true
ROMA_CACHE_DISK=true
ROMA_CACHE_MEMORY=true

# DSPy cache directory
DSPY_CACHE_DIR=.cache/dspy

# ==============================================================================
# Checkpoint Configuration
# ==============================================================================

# Enable checkpointing
ROMA_CHECKPOINT_ENABLED=true

# Checkpoint storage path
ROMA_CHECKPOINT_PATH=.checkpoints

# Maximum checkpoints to keep
ROMA_MAX_CHECKPOINTS=10

# Checkpoint retention (hours)
ROMA_CHECKPOINT_MAX_AGE_HOURS=24.0

# Compress checkpoints
ROMA_CHECKPOINT_COMPRESS=true

# Verify checkpoint integrity
ROMA_CHECKPOINT_VERIFY=true

# ==============================================================================
# Logging Configuration
# ==============================================================================

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL=INFO

# Log directory (null = console only)
LOG_DIR=/app/logs

# Console format (minimal, default, detailed)
LOG_CONSOLE_FORMAT=default

# File format (default, detailed, json)
LOG_FILE_FORMAT=detailed

# Colorize console output
LOG_COLORIZE=true

# JSON serialization
LOG_SERIALIZE=false

# Log rotation size
LOG_ROTATION=100 MB

# Log retention period
LOG_RETENTION=30 days

# Log compression
LOG_COMPRESSION=zip

# Intercept standard library logging
LOG_INTERCEPT_STDLIB=true

# Full traceback on errors
LOG_BACKTRACE=true

# Show variable values in logs (disable in production)
LOG_DIAGNOSE=false

# Thread-safe logging
LOG_ENQUEUE=true

# ==============================================================================
# Observability Configuration (MLflow)
# ==============================================================================

# Enable MLflow tracking
MLFLOW_ENABLED=true

# Disable MLflow span logging without changing other MLflow features
ROMA_DISABLE_MLFLOW_SPANS=false

# MLflow tracking URI (single variable)
# - If running ROMA locally (not in Docker): set to http://127.0.0.1:<port>
# - If running ROMA inside docker-compose: set to http://mlflow:5000
# You can change the host port with MLFLOW_PORT; container port is always 5000.
MLFLOW_TRACKING_URI=http://mlflow:5000

# MLflow experiment name
MLFLOW_EXPERIMENT=ROMA-DSPy

# MLflow port (for docker-compose)
MLFLOW_PORT=5000

# MLflow S3 artifact storage (MinIO)
# - If running locally (not in Docker): set endpoint to http://localhost:9000
# - If running inside docker-compose: set to http://minio:9000 (done automatically)
# These are REQUIRED for artifact storage to work with local MinIO
MLFLOW_DEFAULT_ARTIFACT_ROOT=s3://mlflow
MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
AWS_S3_ENDPOINT_URL=http://localhost:9000

# ==============================================================================
# Observability Configuration (Weights & Biases)
# ==============================================================================

# Weights & Biases (W&B) API Key
# Get your API key from: https://wandb.ai/authorize
# Used for experiment tracking during GEPA optimization (optional)
# If not set, W&B features will be disabled (use_wandb: false in config)
WANDB_API_KEY=your_wandb_api_key

# ==============================================================================
# MinIO Configuration (S3-compatible storage for Docker)
# ==============================================================================

# MinIO root credentials (used by docker-compose for artifact storage)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# MinIO ports
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001

# ==============================================================================
# Docker Configuration
# ==============================================================================

# Ports for docker-compose
POSTGRES_PORT=5432
API_PORT=8000
MLFLOW_PORT=5000

# ==============================================================================
# Optional: Local Development Overrides
# ==============================================================================

# For local development without S3, you can use a local directory
# Uncomment and set to a local path (e.g., ${HOME}/roma_storage)
# Note: This won't work with E2B - only for local testing
# STORAGE_BASE_PATH=${HOME}/roma_storage

# For local development without PostgreSQL
# POSTGRES_ENABLED=false


# Agent LLM timeouts
ROMA__AGENTS__ATOMIZER__LLM__TIMEOUT=120
ROMA__AGENTS__PLANNER__LLM__TIMEOUT=120
ROMA__AGENTS__EXECUTOR__LLM__TIMEOUT=120
ROMA__AGENTS__AGGREGATOR__LLM__TIMEOUT=120
ROMA__AGENTS__VERIFIER__LLM__TIMEOUT=120

# Runtime timeout
ROMA__RUNTIME__TIMEOUT=150

# just solve "收集整理目前中国9阶层实际收入和财务状况，特别研究得出中国的中产有哪些特点，实际中产人数，财力等等"
# just viz b5cf8976-e8c7-4cb7-871a-2d158f6039c9 "" "" "" true