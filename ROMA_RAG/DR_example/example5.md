# 研究报告 #28

## 研究问题

传统的药物研究，即便是从多组学角度出发也难以系统地，宏观地解析药物对机体产生的影响。而且个人异质性会造成其他的影响，因之，请为我调研现阶段大模型是否能模拟药物产生影响来系统性评估药物，这个方向未来会如何发展呢

---

## 研究内容

# 大模型是否已具备“模拟药物对机体系统性影响”的实用能力？—2025现状评估与5–10年路线图

## 执行摘要（结论先行）
- 结论（截至2025）：生物医药领域的大模型（分子/蛋白/细胞/多模态基础模型与多模态LLM）尚未达到“端到端、跨尺度、可泛化到个体”的药物全系统效应仿真；但在关键子任务上已形成“可用模块”，包括分子属性与ADMET预测、体外转录组/单细胞扰动响应拟合、蛋白结构与结合先验、EHR/RWD上的患者分层与结局预测，以及与PBPK/QSP等机理模型的耦合以进行PK/PD与机制层面外推。这些模块组合正逐步支撑药物的系统性评估与部分决策环节（尤其早期筛选与风险控制）[5–7,9,11,12,14–18,21]。  
- 关键原因：大模型提供跨任务可迁移表征与强表达能力；但“系统性影响”需要跨层级（分子—细胞—组织—器官—个体）、跨模态（化学、组学、影像、临床）与因果一致性的数据链条。目前数据联结仍稀疏、偏倚与分布外问题显著，因而必须与机制模型（PBPK/QSP）和因果方法结合，并进行不确定性量化方可用于“决策级”应用[15–18,19,23,32]。  
- 近中期（1–3/3–7年）可落地：  
  1) 以分子/表型基础模型驱动的ADMET与毒性预警、DDI风险分层与处方辅助；  
  2) 结合LINCS/CMap与单细胞扰动的通路/靶点与体外-在体翻译；  
  3) PBPK/QSP+FM的混合“数字孪生”用于剂量优化与亚群体疗效/安全性情境分析；  
  4) 多模态RWD用于适应证扩展与虚拟对照。  
- 远期（7–10年）：多尺度因果基础模型与标准化数字孪生平台，有望在可追溯不确定性与监管可接受的前提下，支持特定病种/药物类别的个体化系统评估与“虚拟临床试验”，但仍依赖纵向多组学与扰动数据扩容、跨机构数据治理与联邦学习、与湿实验闭环的验证体系[15–18,20,23,25,28–29,32–36]。

## 概念与范围
- 大模型（foundation/large models）：以自监督/多任务在大规模数据上学习可迁移表征的模型，覆盖分子语言模型/图模型、蛋白语言模型与结构模型、单细胞/多组学基础模型、跨模态（分子—组学—影像—文本/EHR）模型，以及与机理模型（PBPK/QSP/ODE/PDE）耦合的混合系统[5–7,9,11–13,17–18,23,32]。  
- 模拟药物影响：预测给定干预在分子—细胞—组织—器官—个体层级的作用链与结局（靶点/通路、表达扰动、ADME/PK-PD、疗效终点、毒性与不良反应、DDI/合并用药交互、个体差异）[1–4,7,14–18,21]。  
- 系统性评估：多层级、跨模态、可解释且量化不确定性，具备对新药/新人群/新适应证的受限外推能力，并能进行风险–收益权衡；“决策级”要求外部/前瞻验证与监管可审计[15–18]。  
- 个体异质性：基因组/表观组、微生物组、年龄性别、种族结构、基础疾病与路径差异、环境/行为/社会决定因素等引起的反应差异[20–21].

## 能力现状与任务版图（模型×任务×数据×验证）

### 1) 机制/通路推断与表达/扰动预测（LINCS/CMap、单细胞/空间组学）
- 体外群体均值层面：  
  - 以CMap/LINCS的L1000扰动数据为支撑，深度模型可从化学结构预测药物诱导的基因表达签名，并做“签名逆转”以支持再定位与通路推断。代表性工作在大规模数据上实现中等到较好相关性与Top-k逆转检索性能，但在新细胞系/新药物上的分布外泛化仍受限[1,4].  
  - 例：DLEPS在CMap任务上能从分子结构预测表达谱用于药物再定位，展示横跨药物类别的有效性，但依赖训练分布与细胞背景[4]。  
- 单细胞扰动响应：  
  - 基础模型与生成式方法（scGen/CPA等）可在给定基因/化合物/组合扰动条件下，预测单细胞表达变化与通路层面的响应，已在多数据集上验证组合扰动的可组合性与一定程度的可解释潜变量，但跨细胞类型/疾病状态的外推仍挑战显著[2–3].  
- 空间/多模态：  
  - totalVI等多模态生成模型联合转录与蛋白位点信息，提升细胞状态与通路层面的可解释性，为药物在组织微环境的作用推断提供先验，但缺乏大规模带干预标注的空间扰动数据[12].  
- 对系统评估价值：可作为“分子→细胞/通路”映射与假设生成模块，尤其适合靶点优先级排序、体外组合疗法候选筛选与毒性通路预警；但向器官/个体层级外推需与PBPK/QSP/因果框架耦合[1–3,12,17–18,32].

### 2) ADMET、毒性与PK/PD、DDI
- 分子级属性与毒性：  
  - 化学基础模型（GNN/Transformer、化学语言模型）在MoleculeNet、Tox21/23、TDC等基准上达到或逼近业内可用水准（如Tox21多任务ROC-AUC常见>0.80，最佳可至~0.86–0.88），在溶解度、渗透性等物化/吸收指标与hERG等关键安全端点上表现稳健；深度集成与不确定性定量（如保序/置信学习）可改善“误伤”风险管理[5–9,19].  
- PK/PD与在体转化：  
  - 纯数据驱动的PK/PD拟合（含Neural ODE等）可复刻典型动力学曲线，但在剂量/给药方案/人群结构变化时易出现分布外失真；因此监管与工业界更青睐“PBPK/QSP主导+ML辅助”的混合框架：用PBPK/QSP保证机理一致性与可外推性，用ML学习剩余项（未建模过程/人群协变量非线性）或快速替代昂贵子模块（如组织分配、代谢清除）[15–18,32].  
- DDI与处方级风险：  
  - 知识图谱/表征学习与文本抽取结合的DDI预测能在文献与真实世界信号上达到较高离线AUC，但对新药-新人群-新组合的外推仍需（a）机理先验（酶/转运体饱和、通路交互），（b）FAERS/RWD的外部校准与因果去偏[14,16,30].  
- 对系统评估价值：早期筛选与“不可行方案”排除、临床用药交互风险提示、PBPK/QSP情境分析（人群/合并用药/给药路径）。在决策支持端已具备“有用但非最终依据”的实用价值[5–9,14–18,30].

### 3) 患者分层、应答预测、虚拟对照/虚拟临床试验（EHR/RWD/生物银行）
- 多模态临床预测：  
  - 以OMOP-OHDSI标准化RWD、MIMIC等ICU数据与大型生物银行（UK Biobank）为基础，临床表征学习与LLM可做结局预测、亚群体异质性刻画与辅助分层；但因果混杂与数据漂移导致外部可迁移性与校准成为主要瓶颈[20–21].  
- 虚拟对照/数字孪生：  
  - 目前“数字孪生”多为特定病种/路径（心血管/代谢/免疫）的小范围QSP+统计/ML混合体，用于剂量优化/队列情景分析；要替代真实对照仍需严谨因果识别、先验注册与前瞻验证（监管上通常定位为“支持性证据”，难以单独作为批准依据）[15–18].  
- 对系统评估价值：可用于试验设计（富集、样本量）、疗效终点风险分层、亚群体策略与标签扩展假设生成，但需配套因果与不确定性框架[15–18,20–21].

### 4) 跨模态整合（分子—表型—临床）
- 工具链：化学表征（Chemprop等）、表型/影像（例如高内涵表型RxRx）、单细胞/空间组学整合（totalVI/scVI）、临床结构化数据（OMOP）到文本（临床笔记/文献）[5,12–13,21].  
- 工业实践：NVIDIA BioNeMo等提供端到端可组合的化学/蛋白/基因表达/文本模型组件；Recursion等用表型嵌入连接药物—基因—疾病网络以做表型类比与再定位[13,25].  
- 对系统评估价值：适合作为“证据聚合器”，将分子先验（靶点/结构/ADMET）、表型证据（扰动签名/影像）与临床信号（EHR/RWD）拼接，支撑可信度分级与不确定性分解；核心挑战是跨域对齐与可审计的数据血缘[13,21,25].

### 与传统方法比较
- 相对优势：  
  - 表征迁移与任务覆盖面广；在非线性高维表征与弱标注/自监督场景具有样本效率优势；作为PBPK/QSP的“数据驱动余项”可提升外推与个体化拟合[5–7,11–13,17–18,32].  
- 劣势/风险：  
  - 因果不识别导致的虚假相关；OOD失配与幻觉；可解释性与可审计性弱；与监管对模型变更管理的要求（稳定性/可追溯）有张力[15–18,19,32].

## 个体化与因果推断：技术路径
- 个体异质性融入：  
  - 基因组/表观组与多组学（GTEx、HCA）、微生物组、生活方式与社会决定因素经特征学习后作为PBPK/QSP协变量或作为大模型条件变量；通过生物银行（UKB）与OMOP网络跨机构复现与外部验证[20–21,26–28].  
- 因果与机理耦合：  
  - 以PBPK/QSP编码守恒/结构关系，ML学习剩余项与个体化映射（Physics/Physiology-informed NN）；以因果图/工具变量/双重稳健等手段矫正RWD偏倚，估计个体化处理效应（ITE），并用机理模型进行反事实轨迹约束[17–18,21,32].  
- 不确定性与OOD鲁棒：  
  - 深度集成/贝叶斯近似/保序预测用于置信与风险区间；分布漂移检测与保守迁移；用外部真实世界信号（FAERS、不良事件登记）进行后验校准与告警[19,30].

## 数据与基准：哪些最关键，性能何在
- 关键数据资源：  
  - 体外扰动：LINCS/CMap（化合物/基因扰动表达签名）、大规模CRISPR Perturb-seq[1–2].  
  - 多组学/空间/单细胞：HCA、GTEx、totalVI可对齐的多模态数据[12,26–28].  
  - 化学/生物活性/ADMET：MoleculeNet、Tox21/23、TDC、OGB-MOL[6–9].  
  - 临床/RWD：OMOP-OHDSI网络、MIMIC、FAERS[21,22,30].  
- SOTA与可复现：  
  - 化学性质/毒性：Chemprop等在MoleculeNet/Tox21具领先表现并有可复现实现；TDC提供持续Leaderboard与任务框架[5–7].  
  - 扰动预测：CMap/DLEPS与单细胞scGen/CPA在跨数据集上有对比实验，具备可复现实验代码与数据（多为研究级）[3–4].  
  - DDI：DeepDDI等在文献标注数据集上高AUROC，但对新药与真实世界外部验证有限[14].  
- 前瞻性验证与证据链：  
  - 低成本高通量（多孔板表型/单细胞扰动）→动物/类器官→小规模临床或RWD外部检验，逐层收敛；以保序预测与决策曲线分析报告不确定性与临床净效益[1–4,12–13,19,21].

## 局限、失败模式与边缘场景
- 复杂联合用药与免疫疗法：多通路非线性与时间依赖的相互作用难以被仅数据驱动模型可靠外推，需要QSP/免疫动力学先验与组合扰动数据[2–3,17–18].  
- 长期慢病管理/儿科/老年/罕见病：队列稀疏、治疗路径异质大，外推风险高；需要跨机构联邦学习与先验注册的分析计划[21,22,35].  
- 跨种族/跨机构迁移：分布漂移与编码差异导致性能下降，需要标准化（OMOP）、不变性学习与后验再校准[21].  
- 表征错配与数据融合噪声：多模态对齐缺失与批次效应引入系统性偏倚[12–13].  
- 幻觉与过拟合：多模态LLM在生物医药事实性上存在幻觉风险，应避免将生成文本作为证据，需结合知识库/规则与可追溯链路[25].

## 监管与临床转化（FDA/EMA/NMPA等）
- 监管态势：  
  - FDA发布《药物开发中AI/ML的讨论文件》，强调模型透明度、数据质量、验证计划与变更管理（包括“预定变更控制计划”理念在SaMD领域的延伸）；明确AI/ML可用于药物研发各阶段，但证据等级与风险匹配[15].  
  - FDA针对PBPK发布正式指南，MIDD试点鼓励模型在剂量—暴露—反应—人群外推中的应用，提示PBPK/QSP是“可审计主干”，AI/ML可作为补充证据[17–18].  
  - EMA发布AI在药品全生命周期反思文件，强调人类监督、数据治理与透明性，并鼓励早期与机构沟通[16].  
- 模型卡与变更管理：  
  - 应提供数据血缘、适用边界、性能与不确定性、OOD策略与再训练规则；对高风险决策需预注册验证与独立外部评估[15–18].  
- 产业经验与ROI：  
  - 工业界在早期发现/优化（ADMET/毒性/先导优化）ROI最清晰；递延到临床的证据逐步积累，如表型成像驱动的机理假设与再定位、AI设计药物进入临床（但“AI贡献份额”与因果归因依然需要独立评估）[13,25].

## 未来5–10年技术与应用路线图

### 1–3年（可见且高ROI）
- 技术里程碑：  
  - 化学/蛋白/扰动基础模型的行业级基线与不确定性报告标准；PBPK/QSP+FM耦合工具链（含PINN/残差学习）产品化；跨机构OMOP联邦评测基座搭建[5,11–13,17–18,21,32,35].  
- 数据工程：  
  - 扩容LINCS样本与单细胞组合扰动（含空间）；ADMET与毒性高置信度标签库清洗；RWD语义标准化与因果特征治理[1–3,9,12,21].  
- 应用落地：  
  - ADMET/毒性筛查、DDI风险与处方建议、体外→在体的机制桥接、试验富集/虚拟对照（辅助手段）[5–9,14–18,21].

### 3–7年（系统级拼装）
- 技术里程碑：  
  - 多尺度因果基础模型：在分子—细胞—组织—器官—个体间显式建模因果边界与不确定性传播；数字孪生模板库（病种/治疗路径可复用组件）；闭环实验（主动学习驱动高通量扰动验证）[1–4,12–13,17–18,25,32].  
- 数据工程：  
  - 多中心纵向多组学队列（含干预与转归）、空间扰动图谱与组织微环境时空建模；跨机构数据共享的隐私计算与可追溯治理[12,21,28–29,35].  
- 应用落地：  
  - 指定病种（如免疫/肿瘤部分亚型）的数字孪生用于剂量优化与亚群体策略；再定位与组合疗法的优选与风险—收益权衡（带前瞻检验）[2–3,12,17–18].

### 7–10年（决策级系统性评估）
- 技术里程碑：  
  - 监管可接受的多尺度因果FM+机理混合体，具备端到端不确定性分解与可追溯解释；标准化基准（涵盖跨模态/跨层级/因果外推）与绩效门槛[15–18].  
- 应用落地：  
  - 限定适应证/药物类别的“虚拟临床试验”与个体化系统评估成为常规辅助证据；在标签扩展、儿科/特殊人群剂量、合并用药策略上形成规模化实践[16–18,21].

## 可操作的技术路径与评估框架

- 技术架构（建议）：  
  1) 分子/蛋白基础模型：化学属性—ADMET—毒性—靶点先验（Chemprop/OGB-MOL、ESM/AlphaFold）[5,9,10–11].  
  2) 扰动/表型层：LINCS/CMap签名与单细胞扰动生成（scGen/CPA），空间/多模态对齐（totalVI）[1–3,12].  
  3) 机理主干：PBPK/QSP承担时空传输与通路动力学；FM学习残差与个体化映射（PINN/残差网络）[17–18,32].  
  4) 临床因果：OMOP-RWD中的ITE估计与外部校准；FAERS/不良事件做后验修正与监测[21,30].  
  5) 不确定性与治理：深度集成+保序预测；模型卡/数据卡/变更管理与PCCP样式更新计划[15–19].  
- 评估框架：  
  - 分层指标：分子（ROC-AUC/RMSE+校准）、细胞（表达相关/通路富集一致性）、个体（C-index/校准曲线/反事实一致性）、系统（风险—收益决策曲线）；外部验证与前瞻队列/小型试验验证；不确定性覆盖率目标≥90%并给出OOD退化预案[5–9,1–4,12,15–19,21].

## 近中期可落地的三个以上应用场景（含MVP）
- 早期安全性与可开发性筛选（MVP 3–6个月）：  
  - 目标：在候选库中剔除高毒/差ADMET化合物与高风险DDI组合；  
  - 手段：Chemprop/TDC基线+保序预测；hERG/肝毒/代谢酶抑制多任务模型；DDI知识图谱+FAERS后验校准；  
  - 验证：Tox21外部集、内部体外验证板、回顾性处方数据风险再现[5–9,14,19,30].  
- 靶点/通路与组合疗法优选（MVP 6–9个月）：  
  - 目标：从LINCS/单细胞扰动生成通路级假设并筛选组合；  
  - 手段：scGen/CPA+通路富集；RxRx表型类比；  
  - 验证：体外（Perturb-seq/表型成像）高通量闭环；小动物/类器官转化[1–3,12–13].  
- PBPK/QSP+FM的剂量与人群策略（MVP 9–12个月）：  
  - 目标：在目标适应证内做剂量—人群—合并用药情境分析；  
  - 手段：现有PBPK/QSP模型为主干，FM学习个体协变量映射与残差；  
  - 验证：历史试验拟合+前瞻性模拟—真实一致性检验；在MIDD框架下与监管沟通[17–18].  
- RWD驱动的适应证扩展与虚拟对照（MVP 9–12个月）：  
  - 目标：生成再定位与亚群体获益假设并做虚拟对照；  
  - 手段：OMOP标准化+因果ITE估计+不确定性区间；  
  - 验证：独立机构外部复现+小规模前瞻验证[21].

## 关键风险与合规清单（最小可行验证）
- 风险与对策：  
  - 因果混杂与选择偏倚：先验注册分析计划+工具变量/负对照+灵敏度分析[15–16,21].  
  - OOD与漂移：分布监控+再校准+阈值化“拒识”策略；外部验证必需[19,21].  
  - 可解释与可审计：机理约束+可追溯数据血缘+模型卡/数据卡[15–18].  
  - 隐私与合规：联邦学习/差分隐私与跨机构治理（OMOP），仅在受控用途范围内部署[21,35].  
- MVP最小验证：  
  - 设定明确适用边界（药物类别/病种/终点），预注册指标与决策阈值；  
  - 至少一个外部数据源的独立验证与一次湿实验或类器官闭环；  
  - 报告不确定性覆盖率、决策曲线净效益与失败案例分析；  
  - 建立变更管理（数据/参数/代码）与回滚机制[15–19].

## 直接回答
- 现在是否“可实用地模拟系统性影响并个体化决策”？  
  - 部分可为：作为模块化管线（分子→细胞/通路→PBPK/QSP→个体化因果）中的关键组件，已能支持早期筛选、风险控制与有限范围内的人群/亚群体策略；  
  - 尚不可为：单一端到端大模型在“分子—个体”全链路、跨药物与跨人群的稳健外推与不确定性可审计方面尚不满足“决策级”标准。  
- 最强理由：数据联结与因果/机理一致性仍是瓶颈；混合（PBPK/QSP+FM）与多模态因果是可行主线[15–18,32].  
- 关键限制：跨机构、跨模态的可追溯高质量干预数据稀缺。  
- 下一步：选定高价值场景，构建混合模型+不确定性与因果框架，开展外部与前瞻性验证，并在MIDD/EMA反思框架下早期与监管沟通[15–18].

### Sources
[1] The Connectivity Map (CMap) and CLUE: https://clue.io  
[2] Replogle et al., Combinatorial single-cell CRISPR screening (Perturb-seq), Nat Biotechnol 2020: https://www.nature.com/articles/s41587-020-0456-9  
[3] Lotfollahi et al., scGen predicts single-cell perturbation responses, Nat Methods 2019: https://www.nature.com/articles/s41592-019-0494-8  
[4] Zhang et al., DLEPS: Predicting gene expression profiles of drugs with deep learning, PNAS 2021: https://www.pnas.org/doi/10.1073/pnas.2020070118  
[5] Yang et al., Chemprop/Directed Message Passing for molecular property prediction, Chem Sci 2019: https://pubs.rsc.org/en/content/articlehtml/2019/sc/c8sc02239e  
[6] Therapeutics Data Commons (TDC): https://tdcommons.ai  
[7] MoleculeNet benchmark: https://arxiv.org/abs/1703.00564  
[8] NIH Tox21 Challenge: https://tripod.nih.gov/tox21/challenge/  
[9] Open Graph Benchmark (OGB) – MOL tasks: https://ogb.stanford.edu/docs/graphprop/  
[10] ESM (Evolutionary Scale Modeling) and ESMFold: https://esmatlas.com  
[11] Jumper et al., AlphaFold, Nature 2021: https://www.nature.com/articles/s41586-021-03819-2  
[12] Gayoso et al., totalVI integrates RNA and protein in single cells, Nat Methods 2020: https://www.nature.com/articles/s41592-020-01050-9  
[13] RxRx high-content cellular imaging datasets (Recursion): https://rxrx.ai/rxrx1  
[14] Ryu et al., DeepDDI predicts drug–drug interactions, Sci Rep 2018: https://www.nature.com/articles/s41598-018-33404-8  
[15] FDA, Using Artificial Intelligence & Machine Learning in the Development of Drug and Biological Products (Discussion Paper), 2023: https://www.fda.gov/media/167242/download  
[16] EMA, Reflection paper on the use of AI in the medicinal product lifecycle, 2023: https://www.ema.europa.eu/en/news/ema-publishes-reflection-paper-use-artificial-intelligence-medicinal-product-lifecycle  
[17] FDA Guidance, Physiologically Based Pharmacokinetic Analyses — Format and Content, 2020: https://www.fda.gov/regulatory-information/search-fda-guidance-documents/physiologically-based-pharmacokinetic-analyses-format-and-content  
[18] FDA, Model-Informed Drug Development (MIDD) Pilot Program: https://www.fda.gov/drugs/development-resources/model-informed-drug-development-pilot-program  
[19] Norinder et al., Conformal prediction in chemoinformatics, J Cheminformatics 2021: https://jcheminf.biomedcentral.com/articles/10.1186/s13321-021-00474-9  
[20] UK Biobank resource: https://www.ukbiobank.ac.uk  
[21] OHDSI/OMOP Common Data Model; MIMIC: https://www.ohdsi.org/data-standardization/the-common-data-model/ and https://mimic.mit.edu  
[22] MIMIC-IV (PhysioNet): https://physionet.org/content/mimiciv/  
[23] Raissi et al., Physics-informed neural networks, JCP 2019: https://www.sciencedirect.com/science/article/pii/S0021999118307125  
[24] EPA ToxCast program: https://www.epa.gov/chemical-research/toxicity-forecasting  
[25] NVIDIA BioNeMo for generative AI in biology/chemistry: https://developer.nvidia.com/bionemo  
[26] GTEx Portal: https://gtexportal.org  
[27] Human Cell Atlas: https://www.humancellatlas.org  
[28] Review of spatial transcriptomics methods, Nat Methods 2021: https://www.nature.com/articles/s41592-021-01242-5  
[29] Open Targets Platform: https://www.opentargets.org  
[30] FDA FAERS public dashboard (adverse events): https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html  
[31] BenevolentAI–AstraZeneca collaboration (illustrative industry case): https://www.astrazeneca.com/media-centre/press-releases/2021/astrazeneca-and-benevolentai-collaboration.html  
[32] van der Schaar et al., Causal Inference in Machine Learning for Healthcare, Nat Mach Intell 2020: https://www.nature.com/articles/s42256-020-00236-3  
[33] OHDSI network studies and methods library: https://ohdsi.org  
[34] Insilico Medicine (AI-designed drug clinical pipeline) – news hub: https://insilico.com/news  
[35] Dayan et al., Federated learning for healthcare (EXAM study), Nat Med 2021: https://www.nature.com/articles/s41591-021-01506-3  
[36] EMA guideline landscape on PBPK/QSP and model evaluation (overview page): https://www.ema.europa.eu/en/human-regulatory/research-development/scientific-guidelines/clinical-pharmacology-and-pharmacokinetics-pharmacodynamics-guidelines
