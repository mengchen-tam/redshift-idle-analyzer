# Redshift Idle Time Calculator

[中文版](README.md) | **English**

A simple and efficient tool for analyzing Amazon Redshift cluster idle time and evaluating potential cost savings from migrating to Redshift Serverless.

## 🌟 Features

- 📊 **Dual Analysis**: Provides both IO-level and query-level idle time analysis methods
- ⏱️ **Precise Calculation**: Calculate idle time percentage and active state distribution
- 💰 **Cost Assessment**: Evaluate potential cost savings from Serverless migration
- 🌍 **Region Support**: Theoretically supports all AWS regions, primarily tested and verified in China regions
- 🚀 **Easy to Use**: Single-file script with minimal deployment complexity
- 🔍 **Data Validation**: Built-in data quality checks and permission validation
- 🧪 **Comprehensive Testing**: Complete test suite included
- 📋 **SQL Queries**: Direct SQL analysis scripts for running in Redshift

## 📦 Installation

### System Requirements
- Python 3.7+
- AWS CLI configuration or environment variables

### Installation Steps
1. Download the script file:
```bash
wget https://raw.githubusercontent.com/mengchen-tam/redshift-idle-analyzer/main/redshift_idle_calculator.py
# Or copy the script content directly
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

## 🚀 Usage

### Basic Usage (China Beijing Region)
```bash
python redshift_idle_calculator.py --cluster-id my-cluster
```

### Specify Analysis Period
```bash
python redshift_idle_calculator.py --cluster-id my-cluster --days 14
```

### Other Region Examples
```bash
python redshift_idle_calculator.py --cluster-id my-cluster --region us-east-1 --days 7
```

### Run Tests
```bash
python redshift_idle_calculator.py --test
```

### Use SQL Query Analysis (Optional)
```sql
-- Run in Redshift Query Editor
-- File: redshift_query_idle_analysis.sql
-- Uses accurate gap analysis method
```

## 📋 Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--cluster-id` | ✅ | - | Redshift cluster identifier |
| `--region` | ❌ | cn-north-1 | AWS region |
| `--days` | ❌ | 7 | Analysis days (1-30) |
| `--test` | ❌ | - | Run built-in test suite |
| `--version` | ❌ | - | Show version information |

## 🔐 AWS Permission Requirements

Ensure the AWS credentials running the script have the following permissions:

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

## 📊 Sample Output

```
============================================================
🎯 REDSHIFT IDLE TIME ANALYSIS RESULTS
============================================================

📋 Basic Information:
   Cluster ID: my-redshift-cluster
   AWS Region: cn-north-1
   Cluster Configuration: ra3.xlplus x 2
   Cluster Status: available
   Analysis Period: 2024-01-01 09:00 ~ 2024-01-08 09:00 (7 days)

📊 Data Quality:
   Data Completeness: 95.2%
   Total Data Points: 2016

⏱️  Usage Pattern Analysis:
   Idle Time Percentage: 65.2%
   Active Time Percentage: 34.8%
   Total Time Points: 2016
   Active Time Points: 701
   Idle Time Points: 1315

📈 Active Statistics by Metric:
   ReadIOPS: 450 times (22.3%)
   WriteIOPS: 380 times (18.9%)
   DatabaseConnections: 701 times (34.8%)

💰 Cost Analysis:
   Current Monthly Cost: ¥500.40
   Required Serverless RPU: 8
   Estimated Serverless Cost: ¥224.18
   Potential Monthly Savings: ¥276.22
   Savings Percentage: 55.2%
   Break-even Point: Usage rate needs to be below 77.2%

💡 Recommendations:
   ✅ Strongly recommend migrating to Serverless
      - Can save 55.2% of costs
      - Monthly savings of approximately ¥276.22

⚠️  Important Notes:
   - Cost estimates are based on simplified models, actual costs may vary
   - All prices are hardcoded based on AWS official pricing as of January 2024
   - Global regions (US, Europe, etc.) have not been fully tested, use with caution
   - Serverless has minimum billing units and concurrency limits
   - Recommend testing Serverless performance in non-production environments first
   - Consider data migration and application compatibility
============================================================
```

## 🔍 How It Works

### Dual Analysis Methods

This tool provides two complementary idle time analysis approaches:

#### Method 1: IO-Level Analysis (Python Script)

Analyzes CloudWatch metrics to determine cluster active state:

| Metric | Threshold | Description |
|--------|-----------|-------------|
| ReadIOPS | > 0 | Read I/O operations |
| WriteIOPS | > 0 | Write I/O operations |
| DatabaseConnections | > 0 | Database connection count |

**Characteristics**:
- **Sampling Interval**: Fixed 60 seconds, aligned with Redshift Serverless billing cycle
- **Detection Scope**: Includes all system-level activities (user queries, maintenance, monitoring, etc.)
- **Accuracy**: Reflects actual compute resource usage
- **Use Case**: Serverless migration cost evaluation

#### Method 2: Query-Level Analysis (SQL Script)

Analyzes user query activity based on `sys_query_history` table:

**Characteristics**:
- **Analysis Scope**: Only user SQL query activities
- **Calculation Method**: Conservative estimation based on query time span (Note: does not include detailed gap analysis between queries)
- **Data Source**: Redshift internal query history table
- **Use Case**: Understanding user query patterns and frequency

### Method Comparison

| Dimension | IO-Level Analysis | Query-Level Analysis |
|-----------|-------------------|---------------------|
| **Detection Target** | Resource usage | User query activity |
| **Idle Rate** | Usually lower (e.g., 86%) | Usually higher (e.g., 97%) |
| **Includes** | System maintenance, monitoring, user queries | User queries only |
| **Recommended Use** | Serverless migration decisions | Query pattern analysis |

### 📊 Query-Level Analysis Features

**Accurate Calculation Method**: The current query-level analysis uses gap analysis method to provide accurate idle time calculation:

```
Calculation Logic: 
Total Idle Time = Query Gaps + Time Before First Query + Time After Last Query
Idle Percentage = (Total Idle Time / 24 hours) × 100%

Example Scenario: 1 query every 2 hours for 1 minute each over 24 hours
- Query gaps: 11 × 119 minutes = 21 hours 59 minutes
- Time before/after queries: ~2 hours
- Total idle time: ~23 hours 59 minutes
- Idle percentage: ~99.9%
```

### Cost Calculation Model

1. **Current Cost**: Based on instance type and node count on-demand pricing (dynamic API retrieval)
2. **Serverless Cost**: Active time × RPU hourly rate (dynamic API retrieval)
3. **Savings Calculation**: Current cost - Serverless cost

## 📋 SQL Query Analysis Guide

In addition to the Python script, this tool provides SQL query scripts for query-level idle analysis:

### Usage Steps

1. **Open Redshift Query Editor**
   - Log into AWS Console
   - Navigate to Amazon Redshift service
   - Select your cluster and click "Query data"

2. **Run Analysis Query**
   ```sql
   -- Copy content from redshift_query_idle_analysis.sql and execute
   -- This query uses accurate gap analysis method to calculate idle time between queries
   ```

3. **Interpret Results**
   - **Analysis Period**: Analysis time window (24 hours)
   - **Total Queries**: Total number of queries
   - **Query Span**: Time span from first to last query
   - **Idle Percentage (Conservative)**: Conservative idle percentage estimate
   - **First/Last Query Time**: Query activity time range

### Sample Query Results

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

### Analysis Method Comparison

| Analysis Method | Typical Idle Rate | Use Case |
|----------------|-------------------|----------|
| **IO-Level Analysis** (Python Script) | 80-90% | Serverless migration decisions |
| **Query-Level Analysis** (SQL Script) | 95-99% | Query pattern analysis |

**Recommendation**: Use both methods together - IO-level analysis for cost evaluation, query-level analysis for understanding user activity patterns.

## 🧪 Testing Features

Built-in comprehensive test suite including:

- **Simulated Data Tests**: Verify calculation accuracy under different usage patterns
- **Edge Case Tests**: Test extreme cases like empty data, single data points
- **Input Validation Tests**: Verify parameter validation logic

Run tests:
```bash
python redshift_idle_calculator.py --test
```

## ❓ FAQ

### Q: Why does my analysis show insufficient data?
A: Possible reasons:
- Cluster was stopped during analysis period
- Cluster genuinely had no activity
- CloudWatch metric collection delay
- Insufficient permissions to retrieve certain metrics

**Solution**: Increase analysis days or check cluster status

### Q: How accurate are the cost estimates?
A: Cost estimates are based on simplified models. Actual costs may vary due to:
- AWS pricing changes
- Regional pricing differences
- Actual Serverless pricing model
- Data transfer costs
- Actual query duration, Serverless minimum billing unit is 60s

**Recommendation**: Use results as reference and conduct more detailed cost analysis

### Q: Which Redshift instance types are supported?
A: All mainstream instance types are supported:
- **Recommended RA3 instances**: ra3.large, ra3.xlplus, ra3.4xlarge, ra3.16xlarge
- DC2 instances (being deprecated): dc2.large, dc2.8xlarge
- Note: DC2 instances are being deprecated by AWS, recommend migrating to RA3 instances

### Q: Any special considerations for China regions?
A: 
- Default support for China Beijing region (cn-north-1)
- Support for China Ningxia region (cn-northwest-1)
- Uses RMB pricing based on latest AWS China region prices
- Serverless RPU calculation: 1 RPU = 4 x RA3.XLPlus, minimum 8 RPU
- Ensure using China region AWS credentials

### Q: How is Global region support?
A: **Feature Support**:
- **Theoretical Support**: This tool theoretically supports all AWS regions, including US, Europe, Asia-Pacific and other Global regions
- **Testing Verification**: Primarily tested and verified in China regions (cn-north-1, cn-northwest-1)
- **Core Functions**: 
  - **CloudWatch API**: Standard API calls, supports all regions
  - **Redshift API**: Standard API calls, supports all regions
  - **Pricing API**: Automatically selects correct API endpoint (Global regions use us-east-1)
- **Pricing Data**: 
  - **Dynamic Query**: Prioritizes AWS Pricing API for latest prices
  - **Fallback Prices**: Includes fallback price tables for major Global regions
  - **Auto Fallback**: Automatically uses fallback prices when API fails, ensuring tool availability

**Usage Recommendations**: 
- Global region users can use directly with full functionality
- Recommend testing in test environment before production use
- Tool displays price source (api/hardcoded/default), pay attention to source information
- Use cost estimation results as reference and conduct more detailed cost analysis

## 🔧 Troubleshooting

### Permission Errors
```bash
❌ AWS API Error: An error occurred (AccessDenied)
```
**Solution**: Check IAM permissions, ensure necessary CloudWatch and Redshift permissions

### Cluster Not Found
```bash
❌ Cluster does not exist: my-cluster
```
**Solution**: Check cluster ID spelling and region settings

### Low Data Quality
```bash
⚠️  Data Quality Warning: Insufficient data may affect analysis accuracy
```
**Solution**: Increase analysis days or check if cluster was running normally during analysis period

## 📈 Version History

- **v1.0.0**: Initial release with complete functionality
  - CloudWatch metrics analysis
  - Cost calculation
  - Data quality checks
  - Complete test suite

## 🤝 Contributing

Welcome to submit Issues and Pull Requests to improve this tool!

## 📄 License

MIT License - See LICENSE file for details

## 📞 Support

For questions or suggestions, please:
1. Check the FAQ section
2. Run `--test` to check basic functionality
3. Submit an Issue describing the specific problem