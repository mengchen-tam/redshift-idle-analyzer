# Redshift空闲时间计算器

**中文** | [English](README_EN.md)

一个简单高效的工具，用于分析Amazon Redshift集群的空闲时间，评估迁移到Redshift Serverless的潜在成本节省。

## 🌟 功能特性

- 📊 **双重分析**: 提供IO级别和查询级别两种空闲时间分析方法
- ⏱️ **精确计算**: 计算空闲时间百分比和活跃状态分布
- 💰 **成本评估**: 评估迁移到Serverless的潜在成本节省
- 🌍 **区域支持**: 理论上支持所有AWS区域，主要在中国区域测试验证
- 🚀 **易于使用**: 单文件脚本，无需复杂部署
- 🔍 **数据验证**: 内置数据质量检查和权限验证
- 🧪 **测试完备**: 包含完整的测试套件
- 📋 **SQL查询**: 提供直接在Redshift中运行的查询分析脚本

## 📦 安装

### 系统要求
- Python 3.7+
- AWS CLI 配置或环境变量

### 安装步骤
1. 下载脚本文件：
```bash
wget https://raw.githubusercontent.com/mengchen-tam/redshift-idle-analyzer/main/redshift_idle_calculator.py
# 或直接复制脚本内容
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置AWS凭证：
```bash
aws configure
# 或设置环境变量
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

## 🚀 使用方法

### 基本用法（中国北京区域）
```bash
python redshift_idle_calculator.py --cluster-id my-cluster
```

### 指定分析周期
```bash
python redshift_idle_calculator.py --cluster-id my-cluster --days 14
```

### 其他区域示例
```bash
python redshift_idle_calculator.py --cluster-id my-cluster --region us-east-1 --days 7
```

### 运行测试
```bash
python redshift_idle_calculator.py --test
```

### 使用SQL查询分析（可选）
```sql
-- 在Redshift Query Editor中运行
-- 文件: redshift_query_idle_analysis.sql
-- 使用精确的间隔分析方法
```

## 📋 参数说明

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `--cluster-id` | ✅ | - | Redshift集群标识符 |
| `--region` | ❌ | cn-north-1 | AWS区域 |
| `--days` | ❌ | 7 | 分析天数（1-30） |
| `--test` | ❌ | - | 运行内置测试套件 |
| `--version` | ❌ | - | 显示版本信息 |

## 🔐 AWS权限要求

确保运行脚本的AWS凭证具有以下权限：

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetMetricStatistics",
                "redshift:DescribeClusters",
                "sts:GetCallerIdentity"
            ],
            "Resource": "*"
        }
    ]
}
```

## 📊 输出示例

```
============================================================
🎯 REDSHIFT空闲时间分析结果
============================================================

📋 基本信息:
   集群ID: my-redshift-cluster
   AWS区域: cn-north-1
   集群配置: ra3.xlplus x 2
   集群状态: available
   分析周期: 2024-01-01 09:00 ~ 2024-01-08 09:00 (7天)

📊 数据质量:
   数据完整性: 95.2%
   总数据点: 2016

⏱️  使用模式分析:
   空闲时间百分比: 65.2%
   活跃时间百分比: 34.8%
   总时间点: 2016
   活跃时间点: 701
   空闲时间点: 1315

📈 各指标活跃统计:
   ReadIOPS: 450 次 (22.3%)
   WriteIOPS: 380 次 (18.9%)
   DatabaseConnections: 701 次 (34.8%)

💰 成本分析:
   当前月度成本: ¥500.40
   Serverless所需RPU: 8
   Serverless预估成本: ¥224.18
   潜在月度节省: ¥276.22
   节省百分比: 55.2%
   盈亏平衡点: 使用率需低于 77.2%

💡 建议:
   ✅ 强烈建议迁移到Serverless
      - 可节省 55.2% 的成本
      - 每月可节省约 ¥276.22

⚠️  注意事项:
   - 成本估算基于简化模型，实际成本可能有差异
   - 所有价格均为硬编码，基于2024年1月AWS官方定价
   - Global区域（美国、欧洲等）未经充分测试，建议谨慎使用
   - Serverless有最小计费单位和并发限制
   - 建议在非生产环境先测试Serverless性能
   - 考虑数据迁移和应用程序兼容性
============================================================
```

## 🔍 工作原理

### 双重分析方法

本工具提供两种互补的空闲时间分析方法：

#### 方法1: IO级别分析 (Python脚本)

通过分析CloudWatch指标来判断集群活跃状态：

| 指标 | 阈值 | 说明 |
|------|------|------|
| ReadIOPS | > 0 | 读取I/O操作 |
| WriteIOPS | > 0 | 写入I/O操作 |
| DatabaseConnections | > 0 | 数据库连接数 |

**特点**：
- **采样间隔**: 固定60秒，与Redshift Serverless计费周期一致
- **检测范围**: 包含所有系统级活动（用户查询、后台维护、监控等）
- **准确性**: 反映真实的计算资源使用情况
- **适用场景**: Serverless迁移成本评估

#### 方法2: 查询级别分析 (SQL脚本)

基于`sys_query_history`表分析用户查询活动：

**特点**：
- **分析范围**: 仅统计用户SQL查询活动
- **计算方式**: 基于查询时间跨度的估算
- **数据来源**: Redshift内部查询历史表
- **适用场景**: 了解用户查询模式和频率

### 两种方法的差异

| 维度 | IO级别分析 | 查询级别分析 |
|------|------------|--------------|
| **检测对象** | 底层资源使用 | 用户查询活动 |
| **空闲率** | 通常较低（如86%） | 通常较高（如97%） |
| **包含内容** | 系统维护、监控、用户查询 | 仅用户查询 |
| **推荐用途** | Serverless迁移决策 | 查询模式分析 |

### 📊 查询级别分析特点

**精确计算方法**: 当前的查询级别分析使用间隔分析方法，提供准确的空闲时间计算：

```
计算逻辑: 
总空闲时间 = 查询间隔时间 + 第一个查询前时间 + 最后查询后时间
空闲百分比 = (总空闲时间 / 24小时) × 100%

示例场景: 24小时内每2小时执行1次查询，每次1分钟
- 查询间隔: 11次 × 119分钟 = 21小时59分钟
- 查询前后时间: 约2小时
- 总空闲时间: 约23小时59分钟
- 空闲百分比: 约99.9%
```

### 成本计算模型

1. **当前成本**: 基于实例类型和节点数的按需定价（动态API获取）
2. **Serverless成本**: 活跃时间 × RPU小时费率（动态API获取）
3. **节省计算**: 当前成本 - Serverless成本

## 📋 SQL查询分析使用指南

除了Python脚本，本工具还提供SQL查询脚本进行查询级别的空闲分析：

### 使用步骤

1. **打开Redshift Query Editor**
   - 登录AWS控制台
   - 导航到Amazon Redshift服务
   - 选择你的集群，点击"Query data"

2. **运行分析查询**
   ```sql
   -- 复制 redshift_query_idle_analysis.sql 中的内容并执行
   -- 该查询使用精确的间隔分析方法，计算所有查询间的空闲时间
   ```

3. **解读结果**
   - **Analysis Period**: 分析时间段（24小时）
   - **Total Queries**: 总查询数量
   - **Query Span**: 从第一个查询到最后一个查询的时间跨度
   - **Idle Percentage (Conservative)**: 保守估算的空闲百分比
   - **First/Last Query Time**: 查询活动的时间范围

### 查询结果示例

```
=== REDSHIFT QUERY-LEVEL IDLE ANALYSIS ===
Analysis Period: 24.00 hours
Total Queries: 136 queries
Successful Queries: 118 queries
Total Execution Time: 0.0768 hours
Gaps Between Queries: 0.45 hours
Time Before First Query: 9.31 hours
Time After Last Query: 13.66 hours
Total Idle Time: 23.42 hours
Idle Percentage: 97.59 %
```

### 两种分析方法对比

| 分析方法 | 典型空闲率 | 适用场景 |
|----------|------------|----------|
| **IO级别分析** (Python脚本) | 80-90% | Serverless迁移决策 |
| **查询级别分析** (SQL脚本) | 95-99% | 查询模式分析 |

**建议**: 结合两种方法使用，IO级别分析用于成本评估，查询级别分析用于了解用户活动模式。

## 🧪 测试功能

内置完整的测试套件，包括：

- **模拟数据测试**: 验证不同使用模式下的计算准确性
- **边界情况测试**: 测试空数据、单点数据等极端情况
- **输入验证测试**: 验证参数校验逻辑

运行测试：
```bash
python redshift_idle_calculator.py --test
```

## ❓ 常见问题

### Q: 为什么我的分析结果显示数据不足？
A: 可能的原因：
- 集群在分析期间处于停止状态
- 集群确实没有任何活动
- CloudWatch指标收集延迟
- 权限不足无法获取某些指标

**解决方案**: 增加分析天数或检查集群状态

### Q: 成本估算准确吗？
A: 成本估算基于简化模型，实际成本可能因以下因素有差异：
- AWS定价变化
- 不同区域的定价差异
- Serverless的实际定价模型
- 数据传输成本
- 实际Query的时长，Serverless的最小计费单位是60s

**建议**: 将结果作为参考，进行更详细的成本分析

### Q: 支持哪些Redshift实例类型？
A: 支持所有主流实例类型：
- **推荐使用RA3实例**: ra3.large, ra3.xlplus, ra3.4xlarge, ra3.16xlarge
- DC2实例（即将淘汰）: dc2.large, dc2.8xlarge
- 注意：DC2实例即将被AWS淘汰，建议迁移到RA3实例

### Q: 中国区域有什么特殊考虑？
A: 
- 默认支持中国北京区域 (cn-north-1)
- 支持中国宁夏区域 (cn-northwest-1)
- 使用人民币定价，基于最新的AWS中国区域价格
- Serverless RPU计算：1 RPU = 4 x RA3.XLPlus，最小8 RPU
- 确保使用有权限的AWS凭证

### Q: Global区域支持情况如何？
A: **功能支持说明**：
- **理论支持**: 本工具理论上支持所有AWS区域，包括美国、欧洲、亚太等Global区域
- **测试验证**: 主要在中国区域（cn-north-1, cn-northwest-1）进行了充分测试和验证
- **核心功能**: 
  - **CloudWatch API**: 标准API调用，支持所有区域
  - **Redshift API**: 标准API调用，支持所有区域
  - **Pricing API**: 自动选择正确的API端点（Global区域使用us-east-1）
- **价格数据**: 
  - **动态查询**: 优先使用AWS Pricing API获取最新价格
  - **备用价格**: 包含主要Global区域的备用价格表
  - **自动降级**: API失败时自动使用备用价格，确保工具可用性

**使用建议**: 
- Global区域用户可以直接使用，功能完整
- 建议在生产环境使用前先在测试环境验证
- 工具会显示价格来源（api/hardcoded/default），注意价格来源信息
- 将成本估算结果作为参考，进行更详细的成本分析

## 🔧 故障排除

### 权限错误
```bash
❌ AWS API错误: An error occurred (AccessDenied)
```
**解决**: 检查IAM权限，确保具有必需的CloudWatch和Redshift权限

### 集群不存在
```bash
❌ 集群不存在: my-cluster
```
**解决**: 检查集群ID拼写和区域设置

### 数据质量低
```bash
⚠️  数据质量警告: 数据不足可能影响分析准确性
```
**解决**: 增加分析天数或检查集群在分析期间是否正常运行

## 📈 版本历史

- **v1.0.0**: 初始版本，包含完整功能
  - CloudWatch指标分析
  - 成本计算
  - 数据质量检查
  - 完整测试套件

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个工具！

## 📄 许可证

MIT License - 详见LICENSE文件

## 📞 支持

如有问题或建议，请：
1. 查看常见问题部分
2. 运行 `--test` 检查基本功能
3. 提交Issue描述具体问题