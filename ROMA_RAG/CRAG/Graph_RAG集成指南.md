# Graph RAG 与 CRAG 集成指南

## 📋 概述

**是的，Graph RAG 可以接入 CRAG 方法！** CRAG 的设计理念就是可插拔的，它可以与任何检索系统（包括 Graph RAG）集成。

---

## ✅ 兼容性分析

### CRAG 的接口要求

CRAG 需要的输入格式非常简单：

1. **问题列表** (`sources` 文件)
   - 格式：每行一个问题
   ```
   What is the capital of France?
   Who invented the telephone?
   ```

2. **检索结果** (`retrieved_psgs` 文件)
   - 格式：`问题 [SEP] 检索段落`
   ```
   What is the capital of France? [SEP] Paris is the capital and most populous city of France...
   What is the capital of France? [SEP] France is a country in Western Europe. Its capital is Paris...
   ```

3. **输出要求**
   - CRAG 输出优化后的文本段落（字符串）
   - 这些段落可以直接用于 LLM 生成

### Graph RAG 的输出特点

Graph RAG 通常返回：
- **图结构数据**：节点（实体）和边（关系）
- **子图**：与查询相关的子图结构
- **文本段落**：从图中提取的文本信息

### 兼容性结论

✅ **完全兼容**！只需要将 Graph RAG 的输出转换为 CRAG 要求的格式即可。

---

## 🔧 集成方案

### 方案一：直接文本转换（推荐）

如果 Graph RAG 已经返回文本段落，直接格式化即可：

```python
def graph_rag_to_crag_format(graph_rag_results, queries):
    """
    将 Graph RAG 的检索结果转换为 CRAG 格式
    
    Args:
        graph_rag_results: Graph RAG 返回的检索结果列表
        queries: 问题列表
    
    Returns:
        formatted_results: CRAG 格式的字符串列表
    """
    formatted_results = []
    
    for query, results in zip(queries, graph_rag_results):
        # 如果 Graph RAG 返回的是文本段落列表
        if isinstance(results, list):
            for passage in results:
                formatted_line = f"{query} [SEP] {passage}"
                formatted_results.append(formatted_line)
        # 如果返回的是单个文本
        elif isinstance(results, str):
            formatted_line = f"{query} [SEP] {results}"
            formatted_results.append(formatted_line)
    
    return formatted_results

# 使用示例
graph_results = graph_rag.retrieve(queries)
crag_input = graph_rag_to_crag_format(graph_results, queries)

# 保存为 CRAG 需要的格式
with open('data/dataset/retrieved_psgs', 'w', encoding='utf-8') as f:
    f.write('\n'.join(crag_input))
```

### 方案二：图结构转文本

如果 Graph RAG 返回的是图结构，需要先转换为文本：

```python
def graph_to_text(graph_result, query):
    """
    将图结构转换为文本段落
    
    Args:
        graph_result: Graph RAG 返回的图结构（节点和边）
        query: 查询问题
    
    Returns:
        text_passage: 文本段落
    """
    text_parts = []
    
    # 提取节点信息（实体）
    if 'nodes' in graph_result:
        for node in graph_result['nodes']:
            node_text = f"{node.get('label', '')}: {node.get('description', '')}"
            text_parts.append(node_text)
    
    # 提取边信息（关系）
    if 'edges' in graph_result:
        for edge in graph_result['edges']:
            source = edge.get('source', {}).get('label', '')
            target = edge.get('target', {}).get('label', '')
            relation = edge.get('relation', '')
            edge_text = f"{source} {relation} {target}"
            text_parts.append(edge_text)
    
    # 组合成段落
    passage = '. '.join(text_parts)
    return passage

# 使用示例
graph_results = graph_rag.retrieve(queries)
text_passages = [graph_to_text(result, query) 
                 for result, query in zip(graph_results, queries)]

# 转换为 CRAG 格式
crag_input = [f"{q} [SEP] {p}" for q, p in zip(queries, text_passages)]
```

### 方案三：完整集成流程

完整的 Graph RAG + CRAG 集成流程：

```python
import os
from pathlib import Path

class GraphRAG_CRAG_Integration:
    """Graph RAG 与 CRAG 集成类"""
    
    def __init__(self, graph_rag_system, crag_evaluator_path, crag_generator_path):
        self.graph_rag = graph_rag_system
        self.crag_evaluator_path = crag_evaluator_path
        self.crag_generator_path = crag_generator_path
    
    def retrieve_and_enhance(self, queries, dataset_name='custom'):
        """
        完整的检索和增强流程
        
        Args:
            queries: 问题列表
            dataset_name: 数据集名称
        
        Returns:
            enhanced_results: CRAG 增强后的结果
        """
        # 步骤1: Graph RAG 检索
        print("步骤1: 使用 Graph RAG 进行检索...")
        graph_results = []
        for query in queries:
            result = self.graph_rag.retrieve(query)
            graph_results.append(result)
        
        # 步骤2: 转换为 CRAG 格式
        print("步骤2: 转换 Graph RAG 结果为 CRAG 格式...")
        self._save_crag_format(queries, graph_results, dataset_name)
        
        # 步骤3: 运行 CRAG 知识准备
        print("步骤3: 运行 CRAG 知识准备...")
        self._prepare_crag_knowledge(dataset_name)
        
        # 步骤4: 运行 CRAG 推理
        print("步骤4: 运行 CRAG 推理...")
        enhanced_results = self._run_crag_inference(dataset_name, queries)
        
        return enhanced_results
    
    def _save_crag_format(self, queries, graph_results, dataset_name):
        """保存为 CRAG 格式"""
        data_dir = Path(f"data/{dataset_name}")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存问题列表
        with open(data_dir / 'sources', 'w', encoding='utf-8') as f:
            f.write('\n'.join(queries))
        
        # 转换并保存检索结果
        retrieved_lines = []
        for query, result in zip(queries, graph_results):
            # 将图结果转换为文本
            if isinstance(result, dict):  # 图结构
                passage = self._graph_to_text(result)
            elif isinstance(result, list):  # 文本列表
                passage = ' '.join(result)
            else:  # 单个文本
                passage = str(result)
            
            retrieved_lines.append(f"{query} [SEP] {passage}")
        
        with open(data_dir / 'retrieved_psgs', 'w', encoding='utf-8') as f:
            f.write('\n'.join(retrieved_lines))
    
    def _graph_to_text(self, graph_result):
        """将图结构转换为文本"""
        # 实现图到文本的转换逻辑
        # 这里需要根据您的 Graph RAG 具体实现来调整
        text_parts = []
        
        # 示例：提取节点和边的信息
        if 'entities' in graph_result:
            for entity in graph_result['entities']:
                text_parts.append(f"{entity.get('name', '')}: {entity.get('description', '')}")
        
        if 'relations' in graph_result:
            for rel in graph_result['relations']:
                text_parts.append(f"{rel.get('source', '')} {rel.get('type', '')} {rel.get('target', '')}")
        
        return '. '.join(text_parts) if text_parts else str(graph_result)
    
    def _prepare_crag_knowledge(self, dataset_name):
        """运行 CRAG 知识准备"""
        # 这里调用 CRAG 的知识准备脚本
        import subprocess
        
        scripts_dir = Path("scripts")
        data_dir = Path(f"../data/{dataset_name}")
        
        # 准备内部知识
        subprocess.run([
            'python', str(scripts_dir / 'internal_knowledge_preparation.py'),
            '--model_path', self.crag_evaluator_path,
            '--input_queries', str(data_dir / 'sources'),
            '--input_retrieval', str(data_dir / 'retrieved_psgs'),
            '--decompose_mode', 'selection',
            '--output_file', str(data_dir / 'ref' / 'correct'),
            '--device', 'cuda:0'
        ])
        
        # 准备外部知识（如果需要）
        # ... 类似地调用 external_knowledge_preparation.py
    
    def _run_crag_inference(self, dataset_name, queries):
        """运行 CRAG 推理"""
        import subprocess
        
        scripts_dir = Path("scripts")
        data_dir = Path(f"../data/{dataset_name}")
        
        # 创建输出目录
        (data_dir / 'output').mkdir(exist_ok=True)
        
        # 运行推理
        subprocess.run([
            'python', str(scripts_dir / 'CRAG_Inference.py'),
            '--generator_path', self.crag_generator_path,
            '--evaluator_path', self.crag_evaluator_path,
            '--input_file', str(data_dir / f'test_{dataset_name}.txt'),
            '--output_file', str(data_dir / 'output' / 'crag_output.txt'),
            '--internal_knowledge_path', str(data_dir / 'ref' / 'correct'),
            '--external_knowledge_path', str(data_dir / 'ref' / 'incorrect'),
            '--combined_knowledge_path', str(data_dir / 'ref' / 'ambiguous'),
            '--task', dataset_name,
            '--method', 'crag',
            '--device', 'cuda:0',
            '--ndocs', '10',
            '--upper_threshold', '0.592',
            '--lower_threshold', '0.995'
        ])
        
        # 读取结果
        with open(data_dir / 'output' / 'crag_output.txt', 'r', encoding='utf-8') as f:
            results = [line.strip() for line in f.readlines()]
        
        return results

# 使用示例
# graph_rag = YourGraphRAGSystem()  # 初始化您的 Graph RAG 系统
# integrator = GraphRAG_CRAG_Integration(
#     graph_rag_system=graph_rag,
#     crag_evaluator_path="path/to/evaluator",
#     crag_generator_path="path/to/generator"
# )
# 
# queries = ["What is the capital of France?", "Who invented the telephone?"]
# results = integrator.retrieve_and_enhance(queries, dataset_name='graph_rag_test')
```

---

## 🎯 集成优势

### 1. Graph RAG 的优势
- ✅ 结构化知识表示（实体-关系）
- ✅ 更好的语义理解
- ✅ 支持复杂查询

### 2. CRAG 的增强
- ✅ 评估检索质量
- ✅ 动态知识选择
- ✅ 知识精炼和过滤
- ✅ Web 搜索补充（当图检索不足时）

### 3. 组合效果
```
Graph RAG (结构化检索) 
    ↓
CRAG 评估和增强
    ↓
优化后的知识
    ↓
LLM 生成高质量答案
```

---

## 📝 实际应用示例

### 示例：知识图谱问答系统

```python
# 假设您有一个基于 Neo4j 的 Graph RAG 系统
from neo4j import GraphDatabase

class Neo4jGraphRAG:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def retrieve(self, query):
        """从知识图谱中检索相关信息"""
        with self.driver.session() as session:
            # Cypher 查询获取相关实体和关系
            result = session.run("""
                MATCH (e:Entity)-[r:RELATION]->(e2:Entity)
                WHERE e.name CONTAINS $query OR e.description CONTAINS $query
                RETURN e, r, e2
                LIMIT 10
            """, query=query)
            
            # 转换为文本段落
            passages = []
            for record in result:
                entity1 = record['e']['name']
                relation = record['r']['type']
                entity2 = record['e2']['name']
                passage = f"{entity1} {relation} {entity2}. {record['e']['description']}"
                passages.append(passage)
            
            return ' '.join(passages)

# 集成使用
graph_rag = Neo4jGraphRAG("bolt://localhost:7687", "neo4j", "password")
integrator = GraphRAG_CRAG_Integration(
    graph_rag_system=graph_rag,
    crag_evaluator_path="./models/evaluator",
    crag_generator_path="./models/generator"
)

queries = ["What is the relationship between Einstein and relativity?"]
results = integrator.retrieve_and_enhance(queries)
```

---

## ⚠️ 注意事项

### 1. 数据格式转换
- 确保 Graph RAG 的输出能正确转换为文本段落
- 保留重要的结构化信息（如实体名称、关系类型）

### 2. 性能考虑
- Graph RAG 检索 + CRAG 处理会增加延迟
- 考虑缓存机制，避免重复处理

### 3. 知识准备
- CRAG 的知识准备步骤（correct/incorrect/ambiguous）需要时间
- 可以预先准备，或使用缓存

### 4. 阈值调整
- Graph RAG 的检索质量可能与传统向量检索不同
- 可能需要调整 CRAG 的 `upper_threshold` 和 `lower_threshold`

---

## 🔍 调试建议

### 1. 检查数据格式
```python
# 验证转换后的格式是否正确
with open('data/dataset/retrieved_psgs', 'r') as f:
    lines = f.readlines()[:5]
    for line in lines:
        if ' [SEP] ' not in line:
            print(f"格式错误: {line}")
```

### 2. 可视化对比
- 对比 Graph RAG 原始结果和 CRAG 增强后的结果
- 分析哪些情况下 CRAG 选择了外部知识（Web 搜索）

### 3. 评估指标
- 使用 CRAG 的评估脚本对比效果
- 记录准确率、召回率等指标

---

## 📚 总结

**Graph RAG 完全可以接入 CRAG！** 关键步骤：

1. ✅ 将 Graph RAG 的输出转换为文本格式
2. ✅ 格式化为 CRAG 要求的 `问题 [SEP] 段落` 格式
3. ✅ 运行 CRAG 的知识准备和推理流程
4. ✅ 获得增强后的结果

这种集成可以充分发挥两者的优势：
- **Graph RAG** 提供结构化的知识检索
- **CRAG** 提供智能的质量评估和动态增强

---

**如有具体问题，请参考 CRAG 的代码实现或联系项目维护者。**

