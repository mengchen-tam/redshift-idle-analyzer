# Redshift Idle Time Calculator

[中文版](README.md) | **English**

A simple and efficient tool for analyzing Amazon Redshift cluster idle time and evaluating potential cost savings from migrating to Redshift Serverless.

## 🌟 Features

- 📊 **Smart Analysis**: Analyze actual cluster usage patterns based on multiple CloudWatch metrics
- ⏱️ **Precise Calculation**: Calculate idle time percentage and active state distribution
- 💰 **Cost Assessment**: Evaluate potential cost savings from Serverless migration
- 🌍 **Region Support**: Primarily supports China regions, limited functionality for Global regions
- 🚀 **Easy to Use**: Single-file script with minimal deployment complexity
- 🔍 **Data Validation**: Built-in data quality checks and permission validation
- 🧪 **Comprehensive Testing**: Complete test suite included

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

### Active State Determination Rules

This tool determines cluster active state by analyzing the following CloudWatch metrics:

| Metric | Threshold | Description |
|--------|-----------|-------------|
| ReadIOPS | > 0 | Read I/O operations |
| WriteIOPS | > 0 | Write I/O operations |
| DatabaseConnections | > 0 | Database connection count |

**Sampling Strategy**:
- **Sampling Interval**: Fixed 60 seconds, aligned with Redshift Serverless billing cycle
- **Data Retrieval**: Automatic batch querying to avoid CloudWatch 1440 data point limit
- **Network Traffic**: Not used as active criteria (to avoid misleading system maintenance traffic)

**Active Logic**: A minute is marked as "active" when any metric exceeds its threshold.

### Cost Calculation Model

1. **Current Cost**: Based on instance type and node count on-demand pricing
2. **Serverless Cost**: Active time × hourly rate × 1.2 (assuming 20% premium)
3. **Savings Calculation**: Current cost - Serverless cost

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

### Q: How accurate are Global regions and pricing?
A: **Important Notes**:
- **Testing Scope**: This tool has been thoroughly tested primarily in China regions (cn-north-1, cn-northwest-1)
- **Global Regions**: US, Europe, Asia-Pacific and other Global regions have not been fully tested and may have compatibility issues
- **Pricing Data**: All instance and RPU prices are hardcoded based on AWS official pricing as of January 2024
- **Pricing Accuracy**: 
  - China region prices are relatively accurate and regularly updated
  - Global region prices are for reference only and may differ from actual prices
  - Recommend verifying current AWS official pricing before use

**Recommendations**: 
- Test functionality in a test environment before using in Global regions
- Regularly check AWS official pricing pages to confirm price accuracy
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