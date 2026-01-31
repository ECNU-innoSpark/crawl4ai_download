# ROMA-RAG 服务管理指南

本文档记录了 ROMA-RAG 项目的常用服务管理命令，包括启动、查看日志、关闭等操作。

## 📋 目录

- [服务概览](#服务概览)
- [启动服务](#启动服务)
- [查看服务状态](#查看服务状态)
- [查看日志](#查看日志)
- [停止服务](#停止服务)
- [重启服务](#重启服务)
- [进入容器](#进入容器)
- [数据库操作](#数据库操作)
- [常用任务命令](#常用任务命令)
- [故障排查](#故障排查)

---

## 服务概览

项目包含以下 Docker 服务：

| 服务名称 | 容器名称 | 端口 | 说明 |
|---------|---------|------|------|
| `postgres` | `roma-dspy-postgres` | 5432 | PostgreSQL 数据库 |
| `minio` | `roma-dspy-minio` | 9000, 9001 | MinIO 对象存储（S3兼容） |
| `roma-api` | `roma-dspy-api` | 8000 | ROMA-DSPy API 服务器 |
| `mlflow` | `roma-dspy-mlflow` | 5000 | MLflow 跟踪服务器 |

---

## 启动服务

### 使用 Justfile 命令（推荐）

```bash
# 启动所有服务（包括 MLflow）
just docker-up-full

# 或者使用简化命令
just docker-up
```

### 使用 Docker Compose 命令

```bash
# 启动所有服务（后台运行）
docker compose up -d

# 启动并查看日志（前台运行）
docker compose up

# 启动特定服务
docker compose up -d postgres minio
```

### 首次启动前准备

1. **确保 `.env` 文件存在**
   ```bash
   # 如果不存在，从示例文件复制
   cp env_example.md .env
   # 然后编辑 .env 文件，填入你的配置
   ```

2. **构建 Docker 镜像**
   ```bash
   # 使用 Justfile
   just docker-build
   
   # 或直接使用 Docker Compose
   docker compose build
   ```

---

## 查看服务状态

### 查看所有服务状态

```bash
# 使用 Justfile
just docker-ps

# 使用 Docker Compose
docker compose ps

# 查看详细信息
docker compose ps -a
```

### 检查服务健康状态

```bash
# 检查 API 健康状态
curl http://localhost:8000/health

# 检查 MLflow
curl http://localhost:5000

# 检查 MinIO 控制台
# 浏览器访问: http://localhost:9001
# 默认用户名: minioadmin
# 默认密码: minioadmin123
```

---

## 查看日志

### 查看所有服务日志

```bash
# 使用 Justfile（实时跟踪）
just docker-logs

# 使用 Docker Compose（实时跟踪）
docker compose logs -f

# 查看最近 100 行日志
docker compose logs --tail=100

# 查看最近 10 分钟的日志
docker compose logs --since 10m
```

### 查看特定服务日志

```bash
# 使用 Justfile
just docker-logs-service roma-api
just docker-logs-service postgres
just docker-logs-service mlflow
just docker-logs-service minio

# 使用 Docker Compose
docker compose logs -f roma-api
docker compose logs -f postgres
docker compose logs -f mlflow
docker compose logs -f minio
```

### 查看历史日志

```bash
# 查看所有服务的最后 50 行日志
docker compose logs --tail=50

# 查看特定服务的最后 100 行日志
docker compose logs --tail=100 roma-api

# 查看指定时间范围的日志
docker compose logs --since 2024-01-27T00:00:00
```

---

## 停止服务

### 停止所有服务

```bash
# 使用 Justfile
just docker-down

# 使用 Docker Compose
docker compose down
```

### 停止并清理数据

```bash
# 停止服务并删除数据卷（⚠️ 警告：会删除所有数据）
just docker-down-clean

# 或使用 Docker Compose
docker compose down -v
```

### 停止特定服务

```bash
# 停止单个服务
docker compose stop roma-api

# 停止多个服务
docker compose stop roma-api mlflow
```

---

## 重启服务

### 重启所有服务

```bash
# 使用 Justfile
just docker-restart

# 使用 Docker Compose
docker compose restart
```

### 重启特定服务

```bash
# 重启单个服务
docker compose restart roma-api

# 重启多个服务
docker compose restart roma-api mlflow
```

### 完全重建并重启

```bash
# 停止、重建、启动（使用 Justfile）
just docker-rebuild

# 手动步骤
docker compose down
docker compose build --no-cache
docker compose up -d
```

---

## 进入容器

### 进入 API 容器

```bash
# 使用 Justfile
just docker-shell

# 使用 Docker Compose
docker compose exec roma-api bash

# 或使用容器名称
docker exec -it roma-dspy-api bash
```

### 执行容器内命令

```bash
# 使用 Justfile
just docker-exec <command>

# 使用 Docker Compose
docker compose exec roma-api <command>

# 示例：查看 Python 版本
docker compose exec roma-api python --version
```

### 进入其他容器

```bash
# 进入 PostgreSQL 容器
docker compose exec postgres psql -U postgres -d roma_dspy

# 进入 MinIO 容器
docker compose exec minio sh
```

---

## 数据库操作

### 运行数据库迁移

```bash
# 使用 Justfile
just docker-migrate

# 手动执行
docker compose exec roma-api alembic upgrade head
```

### 连接数据库

```bash
# 使用 psql 连接
docker compose exec postgres psql -U postgres -d roma_dspy

# 或从外部连接（如果端口已映射）
psql -h localhost -p 5432 -U postgres -d roma_dspy
```

### 备份数据库

```bash
# 备份数据库
docker compose exec postgres pg_dump -U postgres roma_dspy > backup.sql

# 备份 MLflow 数据库
docker compose exec postgres pg_dump -U postgres mlflow > mlflow_backup.sql
```

### 恢复数据库

```bash
# 恢复数据库
docker compose exec -T postgres psql -U postgres roma_dspy < backup.sql
```

---

## 常用任务命令

### 运行任务

```bash
# 使用 Justfile 运行任务（如果可用）
just solve "你的任务描述" deep_research 2 false text

# 参数说明：
# - 第一个参数：任务描述
# - profile: 配置文件名（默认: deep_research）
# - max_depth: 最大深度（默认: 2）
# - verbose: 是否详细输出（默认: false）
# - output: 输出格式（默认: text）

# 示例
just solve "请介绍一下海洋教育"

# 如果遇到 WSL 权限错误，直接使用 Docker 命令：
docker exec -it roma-dspy-api roma-dspy solve \
  --profile deep_research \
  --max-depth 2 \
  --output text \
  "你的任务描述"

# 带详细输出的版本
docker exec -it roma-dspy-api roma-dspy solve \
  --profile deep_research \
  --max-depth 2 \
  --output text \
  --verbose \
  "你的任务描述"
```

### 查看执行结果

```bash
# 使用交互式 TUI 查看执行结果
just viz

# 查看特定执行 ID
just viz <execution_id>

# 实时监控
just viz <execution_id> "" "" "" true

# 如果遇到 WSL 权限错误，直接使用 Docker 命令：
docker exec -it roma-dspy-api roma-dspy viz-interactive

# 查看特定执行 ID
docker exec -it roma-dspy-api roma-dspy viz-interactive <execution_id>

# 实时监控模式
docker exec -it roma-dspy-api roma-dspy viz-interactive <execution_id> --live
```

### 访问 Web UI

- **MLflow UI**: http://localhost:5000
- **MinIO 控制台**: http://localhost:9001
  - 用户名: `minioadmin`
  - 密码: `minioadmin123`
- **API 文档**: http://localhost:8000/docs
- **健康检查**: http://localhost:8000/health

---

## MLflow Trace 管理

### 创建新的 Trace

在 MLflow 中，每次执行任务都会自动创建一个新的 trace（run）。以下是几种创建新 trace 的方式：

#### 方法 1：通过执行任务自动创建（推荐）

每次运行 `solve` 命令时，系统会自动创建一个新的 MLflow trace：

```bash
# 每次执行都会创建新的 trace
docker exec -it roma-dspy-api roma-dspy solve \
  --profile deep_research \
  --max-depth 2 \
  --output text \
  "你的任务描述"
```

Trace 名称就是 `execution_id`（自动生成的 UUID）。

#### 方法 2：在代码中手动创建 Trace

如果你在编写自定义代码，可以使用 `MLflowManager.trace_execution()`：

```python
from roma_dspy.core.observability.mlflow_manager import MLflowManager
from roma_dspy.config.schemas.observability import MLflowConfig

# 配置 MLflow
mlflow_config = MLflowConfig(
    enabled=True,
    tracking_uri="http://mlflow:5000",  # Docker 内部使用
    experiment_name="ROMA-DSPy",
    log_traces=True
)

# 初始化 MLflow Manager
mlflow_manager = MLflowManager(mlflow_config)
mlflow_manager.initialize()

# 创建新的 trace
execution_id = "my_custom_execution_001"
with mlflow_manager.trace_execution(
    execution_id=execution_id,
    metadata={
        "custom_param": "value",
        "experiment_type": "custom"
    }
):
    # 在这里执行你的代码
    result = solver.solve(task)
```

#### 方法 3：使用 MLflow 原生 API

直接使用 MLflow 的 `start_run()`：

```python
import mlflow

# 设置实验
mlflow.set_experiment("ROMA-DSPy")

# 创建新的 run（trace）
with mlflow.start_run(run_name="my_custom_run") as run:
    # 记录参数
    mlflow.log_params({
        "max_depth": 2,
        "profile": "deep_research"
    })
    
    # 记录标签
    mlflow.set_tags({
        "execution_id": "custom_001",
        "framework": "ROMA-DSPy"
    })
    
    # 执行你的代码
    result = solver.solve(task)
    
    # 记录指标
    mlflow.log_metrics({
        "total_tasks": 5,
        "completed_tasks": 4
    })
```

#### 方法 4：在已有 Trace 中创建 Span

如果你想在现有的 trace 中添加新的 span：

```python
from mlflow.tracing.fluent import start_span

# 假设你已经在某个 trace 中
with start_span(name="custom_operation") as span:
    span.set_inputs({"input": "data"})
    
    # 执行操作
    result = perform_operation()
    
    span.set_outputs({"output": result})
```

### 查看 Trace

#### 通过 MLflow UI

1. 打开浏览器访问：http://localhost:5000
2. 在左侧选择实验（如 "ROMA-DSPy"）
3. 查看所有 runs（traces）
4. 点击某个 run 查看详细信息

#### 通过命令行

```bash
# 进入容器
docker exec -it roma-dspy-api bash

# 使用 MLflow CLI 查看 runs
mlflow runs list --experiment-id 0

# 查看特定 run 的详细信息
mlflow runs describe <run_id>
```

#### 通过 ROMA TUI

```bash
# 查看所有执行（traces）
docker exec -it roma-dspy-api roma-dspy viz-interactive

# 查看特定执行 ID 的 trace
docker exec -it roma-dspy-api roma-dspy viz-interactive <execution_id>
```

### Trace 命名和组织

#### 自动命名

- 默认使用 `execution_id`（UUID）作为 run name
- 格式：`{execution_id}`（例如：`83c7f31b-c52f-4f79-8a1a-df9d225eb081`）

#### 自定义命名

在代码中创建 trace 时可以指定自定义名称：

```python
with mlflow_manager.trace_execution(
    execution_id="experiment_001",  # 自定义名称
    metadata={"experiment": "test"}
):
    # ...
```

#### 使用标签组织

通过标签（tags）组织 traces：

```python
mlflow.set_tags({
    "experiment_name": "my_experiment",
    "model_version": "v1.0",
    "dataset": "custom_data"
})
```

### 实验管理

#### 创建新实验

```python
import mlflow

# 创建新实验
experiment_id = mlflow.create_experiment(
    name="My Custom Experiment",
    tags={"description": "Custom experiment for testing"}
)
```

#### 切换实验

```python
# 设置当前实验
mlflow.set_experiment("My Custom Experiment")

# 或通过 ID
mlflow.set_experiment(experiment_id=1)
```

#### 查看所有实验

```bash
# 通过 MLflow UI
# 访问 http://localhost:5000，左侧显示所有实验

# 通过 Python API
import mlflow
experiments = mlflow.search_experiments()
for exp in experiments:
    print(f"{exp.name}: {exp.experiment_id}")
```

### 常见问题

**Q: 如何确保每次执行都创建新的 trace？**  
A: 每次调用 `solve` 都会自动创建新的 trace，无需额外操作。

**Q: 如何复用同一个 trace？**  
A: ROMA-DSPy 每次执行都会创建新 trace。如果需要复用，需要手动管理 run 的生命周期。

**Q: Trace 数据存储在哪里？**  
A: 
- 元数据：PostgreSQL（通过 MLflow backend store）
- 追踪数据：PostgreSQL（MLflow traces）
- 文件/模型：MinIO（S3 兼容存储）

**Q: 如何删除旧的 trace？**  
A: 通过 MLflow UI 删除，或使用 API：
```python
import mlflow
mlflow.delete_run(run_id="<run_id>")
```

---

## 故障排查

### 服务无法启动

1. **检查 Docker 是否运行**
   ```bash
   docker ps
   ```

2. **检查端口是否被占用**
   ```bash
   # Windows
   netstat -ano | findstr :8000
   netstat -ano | findstr :5432
   
   # Linux/Mac
   lsof -i :8000
   lsof -i :5432
   ```

3. **检查 .env 文件**
   ```bash
   # 确保 .env 文件存在
   ls -la .env
   ```

4. **查看错误日志**
   ```bash
   docker compose logs --tail=50
   ```

### 服务启动但无法访问

1. **检查服务状态**
   ```bash
   docker compose ps
   ```

2. **检查服务健康状态**
   ```bash
   curl http://localhost:8000/health
   ```

3. **查看服务日志**
   ```bash
   docker compose logs -f roma-api
   ```

### 清理并重新开始

```bash
# 停止所有服务
docker compose down

# 删除所有容器、网络和数据卷
docker compose down -v

# 清理未使用的镜像
docker system prune -a

# 重新构建并启动
just docker-rebuild
```

### 常见错误解决

1. **端口冲突**
   - 修改 `.env` 文件中的端口配置
   - 或停止占用端口的其他服务
   - 检查端口占用：
     ```bash
     # Windows
     netstat -ano | findstr :9000
     netstat -ano | findstr :9001
     ```

2. **WSL 权限错误（`Permission denied (os error 13)`）**
   
   如果在 WSL 环境中遇到 `I/O error reading directory '/var/lib/snapd/void': Permission denied` 错误：
   
   **解决方案 1：直接使用 Docker Compose 命令（推荐）**
   ```bash
   # 替代 just docker-up-full
   docker compose up -d
   
   # 替代 just docker-down
   docker compose down
   
   # 替代 just solve
   docker exec -it roma-dspy-api roma-dspy solve --profile deep_research --max-depth 2 --output text "你的任务"
   
   # 替代 just docker-logs
   docker compose logs -f
   
   # 替代 just docker-ps
   docker compose ps
   ```
   
   **解决方案 2：修复 just 工具权限**
   ```bash
   # 在 WSL 中，可以尝试忽略该目录
   export JUST_IGNORE_ERRORS=true
   
   # 或者使用 Windows 版本的 just（如果可用）
   ```
   
   **解决方案 3：使用 PowerShell 而不是 WSL**
   - 在 Windows PowerShell 中运行命令，避免 WSL 权限问题

3. **权限问题**
   - 确保 Docker 有足够权限
   - 检查文件挂载权限
   - 在 WSL 中，确保 Docker Desktop 已正确配置

4. **内存不足**
   - 检查 Docker Desktop 资源分配
   - 增加 Docker 可用内存

5. **`ragflow-sdk` 未安装错误**
   
   如果看到错误 `ragflow-sdk is not installed`：
   
   **解决方案 1：在容器内手动安装（临时修复）**
   ```bash
   # 进入容器
   docker exec -it roma-dspy-api bash
   
   # 安装 ragflow-sdk
   pip install ragflow-sdk
   
   # 退出容器
   exit
   
   # 重启服务
   docker compose restart roma-api
   ```
   
   **解决方案 2：重新构建镜像（永久修复）**
   ```bash
   # 停止服务
   docker compose down
   
   # 清除缓存并重新构建
   docker compose build --no-cache
   
   # 启动服务
   docker compose up -d
   ```
   
   **解决方案 3：如果不需要 RAGFlow，可以禁用该工具包**
   
   编辑 `config/profiles/deep_research.yaml`，将 `AdaptiveRetrieveToolkit` 的 `enabled` 设为 `false`，或移除该工具包配置。

6. **RAGFlow 配置缺失**
   
   确保 `.env` 文件中配置了以下变量：
   ```bash
   RAGFLOW_API_URL=http://host.docker.internal:8080/api/v1
   RAGFLOW_API_KEY=your-ragflow-api-key
   RAGFLOW_KB_ID=your-knowledge-base-id
   ```

---

## 快速参考

### 最常用命令

**使用 Justfile（如果可用）：**
```bash
# 启动服务
just docker-up-full

# 查看日志
just docker-logs

# 查看状态
just docker-ps

# 停止服务
just docker-down

# 运行任务
just solve "任务描述"

# 进入容器
just docker-shell
```

**直接使用 Docker Compose（WSL 权限问题时的替代方案）：**
```bash
# 启动服务
docker compose up -d

# 查看日志
docker compose logs -f

# 查看状态
docker compose ps

# 停止服务
docker compose down

# 运行任务
docker exec -it roma-dspy-api roma-dspy solve --profile deep_research --max-depth 2 --output text "任务描述"

# 进入容器
docker compose exec roma-api bash
```

### 服务访问地址

- API: http://localhost:8000
- MLflow: http://localhost:5000
- MinIO: http://localhost:9000
- MinIO Console: http://localhost:9001
- PostgreSQL: localhost:5432

---

## 注意事项

1. **首次启动**：确保已创建并配置 `.env` 文件
2. **数据持久化**：数据存储在 Docker volumes 中，删除容器不会丢失数据
3. **资源占用**：确保系统有足够的内存和 CPU 资源
4. **网络**：确保防火墙允许相关端口访问
5. **备份**：定期备份数据库和重要数据

---

## 更多信息

- 详细配置说明：查看 `env_example.md`
- 配置文件：查看 `config/` 目录
- 完整命令列表：运行 `just --list`
- 项目文档：查看 `docs/` 目录
