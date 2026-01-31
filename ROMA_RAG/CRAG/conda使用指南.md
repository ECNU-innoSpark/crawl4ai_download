# Conda 在 PowerShell 中的使用指南

## 🔍 问题诊断

您已经安装了 Anaconda（位于 `D:\Anaconda`），但在 PowerShell 中无法直接使用 `conda` 命令。这是因为 conda 需要在 PowerShell 中初始化。

## ✅ 解决方案

### 方法一：使用 Anaconda Prompt（最推荐！）

**这是最简单可靠的方法**：

1. 按 `Win` 键，搜索 "**Anaconda Prompt**"
2. 右键点击 "Anaconda Prompt"，选择"以管理员身份运行"（可选）
3. 在这个终端中，conda 命令可以直接使用，无需任何配置

**优点**：
- ✅ 无需配置
- ✅ 开箱即用
- ✅ 避免路径问题

### 方法二：初始化 Conda

如果 conda.exe 存在，可以尝试初始化：

```powershell
# 方法2a: 使用 Scripts 目录（如果存在）
& "D:\Anaconda\Scripts\conda.exe" init powershell

# 方法2b: 使用 condabin 目录（如果存在）
& "D:\Anaconda\condabin\conda.bat" init powershell

# 方法2c: 使用 Python 模块方式
python -m conda init powershell
```

**注意**：如果上述命令都报错"无法识别"，说明 conda 可执行文件可能不存在或路径不正确。

初始化后，**关闭并重新打开 PowerShell**，conda 命令就可以使用了。

### 方法三：手动添加 PATH（临时解决）

如果 conda 文件存在但不在 PATH 中，可以临时添加：

```powershell
# 添加 conda 相关路径到当前会话
$env:Path = "D:\Anaconda;D:\Anaconda\Scripts;D:\Anaconda\condabin;" + $env:Path

# 现在尝试使用 conda
conda --version
```

**注意**：这只是临时添加，关闭 PowerShell 后失效。

### 方法四：使用完整路径（如果文件存在）

如果 conda 文件存在，可以直接使用完整路径：

```powershell
# 尝试不同的可能路径
# 路径1: Scripts 目录
& "D:\Anaconda\Scripts\conda.exe" --version

# 路径2: condabin 目录
& "D:\Anaconda\condabin\conda.bat" --version

# 路径3: 使用 Python 模块
python -m conda --version
```

**如果所有路径都失败**，说明 conda 可能未正确安装，建议：
1. 重新安装 Anaconda/Miniconda
2. 或使用 Anaconda Prompt（推荐）

## 🚀 创建 CRAG 环境

### 在 Anaconda Prompt 中（推荐）

```bash
# 1. 打开 Anaconda Prompt
# 2. 导航到项目目录
cd "D:\OneDrive\桌面\教育大模型\CRAG\CRAG"

# 3. 创建环境
conda create -n CRAG python=3.11

# 4. 激活环境
conda activate CRAG

# 5. 安装依赖
pip install -r requirements.txt
```

### 如果 conda 命令不可用

如果无法使用 conda，可以直接使用系统的 Python（您已安装 Python 3.11）：

```powershell
# 使用系统的 Python 3.11 创建虚拟环境
python -m venv CRAG_env

# 激活虚拟环境（PowerShell）
.\CRAG_env\Scripts\Activate.ps1

# 如果激活失败，可能需要设置执行策略
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# 安装依赖
pip install -r requirements.txt
```

## 📝 验证安装

创建环境后，验证是否成功：

```bash
# 检查 conda 版本
conda --version

# 查看所有环境
conda env list

# 激活环境后检查 Python 版本
conda activate CRAG
python --version  # 应该显示 Python 3.11.x
```

## ⚠️ 常见问题

### 问题1：conda.exe 或 conda.bat 找不到

**症状**：`无法将"D:\Anaconda\Scripts\conda.exe"项识别为 cmdlet...`

**原因**：
- conda 可执行文件可能不存在
- Anaconda 安装不完整
- 路径不正确

**解决方案**：
1. **最简单**：使用 Anaconda Prompt（推荐）
2. **检查安装**：确认 `D:\Anaconda` 目录下是否有 `Scripts` 或 `condabin` 目录
3. **重新安装**：如果文件确实不存在，考虑重新安装 Anaconda 或 Miniconda

### 问题2：初始化后仍然无法使用

**解决**：
1. 确保完全关闭并重新打开 PowerShell
2. 检查 PowerShell 执行策略：
   ```powershell
   Get-ExecutionPolicy
   ```
   如果显示 `Restricted`，需要改为 `RemoteSigned`：
   ```powershell
   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

### 问题2：路径中有中文字符

如果您的用户名或路径包含中文，可能会导致问题。可以：
- 使用 Anaconda Prompt（推荐）
- 或者使用完整路径调用 conda

### 问题3：conda 命令很慢

**解决**：
```bash
# 更新 conda
conda update conda

# 清理缓存
conda clean --all
```

## 🎯 推荐工作流程

### 方案A：使用 Anaconda Prompt（强烈推荐）

1. **打开 Anaconda Prompt**
   - 按 `Win` 键，搜索 "Anaconda Prompt"
   - 右键选择"以管理员身份运行"（可选）

2. **导航到项目目录**：
   ```bash
   cd "D:\OneDrive\桌面\教育大模型\CRAG\CRAG"
   ```

3. **创建并激活环境**：
   ```bash
   conda create -n CRAG python=3.11
   conda activate CRAG
   ```

4. **安装依赖**：
   ```bash
   pip install -r requirements.txt
   ```

### 方案B：使用 Python venv（如果 conda 不可用）

1. **在 PowerShell 中导航到项目**：
   ```powershell
   cd "D:\OneDrive\桌面\教育大模型\CRAG\CRAG"
   ```

2. **创建虚拟环境**：
   ```powershell
   python -m venv CRAG_env
   ```

3. **激活虚拟环境**：
   ```powershell
   .\CRAG_env\Scripts\Activate.ps1
   ```
   如果提示执行策略错误，运行：
   ```powershell
   Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

4. **安装依赖**：
   ```powershell
   pip install -r requirements.txt
   ```

## 💡 快速检查脚本

创建一个 PowerShell 脚本来检查 conda：

```powershell
# check_conda.ps1
$condaPaths = @(
    "D:\Anaconda\Scripts\conda.exe",
    "D:\Anaconda\condabin\conda.bat",
    "$env:USERPROFILE\Anaconda3\Scripts\conda.exe"
)

foreach ($path in $condaPaths) {
    if (Test-Path $path) {
        Write-Host "找到 conda: $path" -ForegroundColor Green
        & $path --version
        break
    }
}

if (-not (Get-Command conda -ErrorAction SilentlyContinue)) {
    Write-Host "`nconda 未在 PATH 中，请初始化：" -ForegroundColor Yellow
    Write-Host "& 'D:\Anaconda\Scripts\conda.exe' init powershell" -ForegroundColor Cyan
}
```

保存为 `check_conda.ps1` 并运行：
```powershell
.\check_conda.ps1
```

## 🔧 诊断脚本

创建一个诊断脚本来检查 conda 状态：

```powershell
# 保存为 check_conda.ps1
Write-Host "=== Conda 诊断 ===" -ForegroundColor Cyan

# 检查 Anaconda 目录
$anacondaPath = "D:\Anaconda"
if (Test-Path $anacondaPath) {
    Write-Host "✓ Anaconda 目录存在: $anacondaPath" -ForegroundColor Green
} else {
    Write-Host "✗ Anaconda 目录不存在: $anacondaPath" -ForegroundColor Red
}

# 检查可能的 conda 文件
$possiblePaths = @(
    "$anacondaPath\Scripts\conda.exe",
    "$anacondaPath\condabin\conda.bat",
    "$anacondaPath\Scripts\conda-script.py"
)

Write-Host "`n检查 conda 文件:" -ForegroundColor Yellow
foreach ($path in $possiblePaths) {
    if (Test-Path $path) {
        Write-Host "  ✓ 找到: $path" -ForegroundColor Green
    } else {
        Write-Host "  ✗ 不存在: $path" -ForegroundColor Gray
    }
}

# 检查 PATH
Write-Host "`n检查 PATH 中的 conda 路径:" -ForegroundColor Yellow
$condaInPath = $env:Path -split ';' | Where-Object { $_ -like '*conda*' -or $_ -like '*Anaconda*' }
if ($condaInPath) {
    $condaInPath | ForEach-Object { Write-Host "  ✓ $_" -ForegroundColor Green }
} else {
    Write-Host "  ✗ PATH 中未找到 conda 相关路径" -ForegroundColor Red
}

# 检查 Python
Write-Host "`n检查 Python:" -ForegroundColor Yellow
try {
    $pythonPath = python -c "import sys; print(sys.executable)" 2>&1
    Write-Host "  Python 路径: $pythonPath" -ForegroundColor Green
    if ($pythonPath -like '*Anaconda*') {
        Write-Host "  ✓ 使用的是 Anaconda 的 Python" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ 使用的是系统 Python，不是 Anaconda 的" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ✗ 无法获取 Python 信息" -ForegroundColor Red
}

Write-Host "`n=== 建议 ===" -ForegroundColor Cyan
Write-Host "如果 conda 文件不存在，请使用 Anaconda Prompt 或重新安装 Anaconda" -ForegroundColor Yellow
```

运行诊断：
```powershell
.\check_conda.ps1
```

---

## 💡 最终建议

**如果 conda 命令无法使用，有两个选择：**

1. **使用 Anaconda Prompt**（最简单）
   - 这是 Anaconda 自带的终端
   - conda 命令开箱即用
   - 无需任何配置

2. **使用 Python venv**（如果 conda 确实不可用）
   - 您已经安装了 Python 3.11
   - 使用 `python -m venv` 创建虚拟环境
   - 功能类似，只是管理工具不同

**对于 CRAG 项目，两种方法都可以正常工作！**

