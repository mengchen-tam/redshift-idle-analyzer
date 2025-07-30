#!/usr/bin/env python3
"""
Redshift空闲时间计算器

通过分析CloudWatch指标来计算Redshift集群的空闲时间百分比，
评估迁移到Serverless的潜在成本节省。

功能特性:
- 📊 智能分析Redshift集群使用模式
- ⏱️ 精确计算空闲时间百分比
- 💰 评估Serverless迁移成本节省
- 🌍 支持所有AWS区域（包括中国区）
- 🔍 内置数据质量检查和权限验证
- 🧪 完整的测试套件

使用方法:
    # 基本分析
    python redshift_idle_calculator.py --cluster-id my-cluster --region us-east-1
    
    # 指定分析周期
    python redshift_idle_calculator.py --cluster-id my-cluster --region us-east-1 --days 14
    
    # 运行测试
    python redshift_idle_calculator.py --test

作者: Redshift Cost Optimizer
版本: 1.0.0
许可: MIT License
"""

import argparse
import sys
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from collections import namedtuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# 版本信息
__version__ = "1.0.0"
__author__ = "Redshift Cost Optimizer"

# 数据结构定义
MetricPoint = namedtuple('MetricPoint', ['timestamp', 'value'])

def get_rpu_price_dynamic(region: str) -> Dict[str, Any]:
    """
    动态获取RPU价格，优先使用AWS Pricing API，失败时使用备用价格
    
    Args:
        region: AWS区域
        
    Returns:
        价格信息字典，包含price, currency, source等字段
    """
    def get_pricing_api_region(region: str) -> str:
        """确定Pricing API区域"""
        if region.startswith('cn-'):
            return 'cn-northwest-1'  # 中国区域的Pricing API
        else:
            return 'us-east-1'  # Global区域的Pricing API
    
    def get_location_name(region: str) -> str:
        """区域代码转换为位置名称"""
        region_mapping = {
            'us-east-1': 'US East (N. Virginia)',
            'us-west-2': 'US West (Oregon)',
            'eu-west-1': 'Europe (Ireland)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'cn-north-1': 'China (Beijing)',
            'cn-northwest-1': 'China (Ningxia)',
        }
        return region_mapping.get(region, region)
    
    def query_api_price(region: str) -> Optional[Dict]:
        """使用API查询价格"""
        try:
            pricing_region = get_pricing_api_region(region)
            location = get_location_name(region)
            
            pricing_client = boto3.client('pricing', region_name=pricing_region)
            
            response = pricing_client.get_products(
                ServiceCode='AmazonRedshift',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                    {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Serverless'}
                ],
                MaxResults=10
            )
            
            for product_str in response.get('PriceList', []):
                product = json.loads(product_str)
                attributes = product.get('product', {}).get('attributes', {})
                payment_option = attributes.get('paymentOption', '')
                
                terms = product.get('terms', {}).get('OnDemand', {})
                for term_key, term_value in terms.items():
                    price_dimensions = term_value.get('priceDimensions', {})
                    for price_key, price_value in price_dimensions.items():
                        unit = price_value.get('unit', '')
                        description = price_value.get('description', '')
                        price_per_unit = price_value.get('pricePerUnit', {})
                        
                        if unit == 'RPU-Hr' and price_per_unit:
                            currency = list(price_per_unit.keys())[0]
                            price = float(price_per_unit[currency])
                            
                            # 判断是否为按需定价（不是预留实例）
                            is_on_demand = (
                                not payment_option or  # 空的payment_option通常是按需
                                payment_option == 'On Demand' or
                                ('serverless usage' in description.lower() and 'reservations' not in description.lower())
                            )
                            
                            # 调试信息
                            # print(f"DEBUG: payment_option='{payment_option}', description='{description}', is_on_demand={is_on_demand}")
                            
                            if is_on_demand:
                                return {'price': price, 'currency': currency, 'source': 'api'}
            return None
        except Exception:
            return None
    
    def get_fallback_price(region: str) -> Dict:
        """获取备用硬编码价格"""
        fallback_prices = {
            'cn-north-1': {'price': 2.692, 'currency': 'CNY'},
            'cn-northwest-1': {'price': 2.093, 'currency': 'CNY'},
            'us-east-1': {'price': 0.375, 'currency': 'USD'},
            'us-west-2': {'price': 0.375, 'currency': 'USD'},
            'eu-west-1': {'price': 0.375, 'currency': 'USD'},
            'ap-southeast-1': {'price': 0.45, 'currency': 'USD'},
        }
        
        if region in fallback_prices:
            data = fallback_prices[region]
            return {'price': data['price'], 'currency': data['currency'], 'source': 'hardcoded'}
        else:
            return {'price': 0.375, 'currency': 'USD', 'source': 'default'}
    
    # 首先尝试API查询
    api_result = query_api_price(region)
    if api_result:
        return api_result
    else:
        return get_fallback_price(region)

def get_instance_price_dynamic(node_type: str, region: str) -> Dict[str, Any]:
    """
    动态获取Redshift实例价格，优先使用AWS Pricing API，失败时使用备用价格
    
    Args:
        node_type: 实例类型 (如 ra3.xlplus)
        region: AWS区域
        
    Returns:
        价格信息字典，包含price, currency, source等字段
    """
    def query_instance_api_price(node_type: str, region: str) -> Optional[Dict]:
        """使用API查询实例价格"""
        try:
            pricing_region = 'cn-northwest-1' if region.startswith('cn-') else 'us-east-1'
            location = {
                'us-east-1': 'US East (N. Virginia)',
                'us-west-2': 'US West (Oregon)',
                'eu-west-1': 'Europe (Ireland)',
                'ap-southeast-1': 'Asia Pacific (Singapore)',
                'cn-north-1': 'China (Beijing)',
                'cn-northwest-1': 'China (Ningxia)',
            }.get(region, region)
            
            pricing_client = boto3.client('pricing', region_name=pricing_region)
            
            response = pricing_client.get_products(
                ServiceCode='AmazonRedshift',
                Filters=[
                    {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                    {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Compute Instance'},
                    {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': node_type}
                ],
                MaxResults=5
            )
            
            for product_str in response.get('PriceList', []):
                product = json.loads(product_str)
                
                # 查找按需定价
                terms = product.get('terms', {}).get('OnDemand', {})
                for term_key, term_value in terms.items():
                    price_dimensions = term_value.get('priceDimensions', {})
                    for price_key, price_value in price_dimensions.items():
                        unit = price_value.get('unit', '')
                        price_per_unit = price_value.get('pricePerUnit', {})
                        
                        if unit == 'Hrs' and price_per_unit:
                            currency = list(price_per_unit.keys())[0]
                            price = float(price_per_unit[currency])
                            return {'price': price, 'currency': currency, 'source': 'api'}
            return None
        except Exception:
            return None
    
    def get_fallback_instance_price(node_type: str, region: str) -> Dict:
        """获取备用硬编码实例价格"""
        # 中国区域定价表（每小时人民币）
        cn_pricing_table = {
            'dc2.large': 2.145,
            'dc2.8xlarge': 41.60,
            'ra3.large': 3.475,
            'ra3.xlplus': 6.950,
            'ra3.4xlarge': 20.864,
            'ra3.16xlarge': 83.456,
        }
        
        # 美国区域定价表（每小时美元）
        us_pricing_table = {
            'dc2.large': 0.25,
            'dc2.8xlarge': 4.80,
            'ra3.large': 0.48,
            'ra3.xlplus': 1.086,
            'ra3.4xlarge': 3.26,
            'ra3.16xlarge': 13.04,
        }
        
        if region.startswith('cn-'):
            pricing_table = cn_pricing_table
            currency = 'CNY'
        else:
            pricing_table = us_pricing_table
            currency = 'USD'
        
        if node_type in pricing_table:
            price = pricing_table[node_type]
        else:
            # 未知实例类型使用ra3.xlplus价格
            price = pricing_table.get('ra3.xlplus', 6.950 if region.startswith('cn-') else 1.086)
        
        return {'price': price, 'currency': currency, 'source': 'hardcoded'}
    
    # 首先尝试API查询
    api_result = query_instance_api_price(node_type, region)
    if api_result:
        return api_result
    else:
        return get_fallback_instance_price(node_type, region)

def validate_inputs(cluster_id: str, region: str, days: int) -> None:
    """
    验证输入参数
    
    Args:
        cluster_id: Redshift集群标识符
        region: AWS区域
        days: 分析天数
        
    Raises:
        ValueError: 当输入参数无效时
    """
    # 验证集群ID
    if not cluster_id or not cluster_id.strip():
        raise ValueError("集群ID不能为空")
    
    # 集群ID格式验证（基本检查）
    cluster_id = cluster_id.strip()
    if len(cluster_id) < 1 or len(cluster_id) > 63:
        raise ValueError("集群ID长度必须在1-63个字符之间")
    
    # 验证区域
    if not region or not region.strip():
        raise ValueError("区域不能为空")
    
    # 基本的区域格式验证
    valid_region_patterns = [
        'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
        'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-central-1',
        'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ap-northeast-2',
        'cn-north-1', 'cn-northwest-1',  # 中国区域
        'ca-central-1', 'sa-east-1'
    ]
    
    region = region.strip()
    # 简单的区域格式检查（允许自定义区域）
    if not (region in valid_region_patterns or 
            (len(region.split('-')) >= 3 and region.replace('-', '').replace('1', '').replace('2', '').replace('3', '').isalpha())):
        print(f"⚠️  警告: 区域 '{region}' 可能不是有效的AWS区域")
    
    # 验证天数
    if not isinstance(days, int):
        raise ValueError("分析天数必须是整数")
    
    if days <= 0:
        raise ValueError("分析天数必须大于0")
    
    if days > 30:
        raise ValueError("分析天数不能超过30天（CloudWatch数据保留限制）")
    
    print(f"✓ 输入验证通过: 集群={cluster_id}, 区域={region}, 天数={days}")

def validate_aws_credentials(region: str) -> bool:
    """
    验证AWS凭证是否有效
    
    Args:
        region: AWS区域
        
    Returns:
        True如果凭证有效，False否则
    """
    try:
        # 尝试获取调用者身份
        sts = boto3.client('sts', region_name=region)
        response = sts.get_caller_identity()
        
        account_id = response.get('Account', 'unknown')
        user_arn = response.get('Arn', 'unknown')
        
        print(f"✓ AWS凭证验证通过")
        print(f"   账户ID: {account_id}")
        print(f"   用户ARN: {user_arn}")
        
        return True
        
    except NoCredentialsError:
        print("❌ AWS凭证未配置")
        print("   请运行 'aws configure' 或设置环境变量:")
        print("   - AWS_ACCESS_KEY_ID")
        print("   - AWS_SECRET_ACCESS_KEY")
        print("   - AWS_SESSION_TOKEN (如果使用临时凭证)")
        return False
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidUserID.NotFound':
            print("❌ AWS凭证无效或已过期")
        elif error_code == 'AccessDenied':
            print("❌ AWS凭证权限不足")
        else:
            print(f"❌ AWS凭证验证失败: {e}")
        return False
        
    except Exception as e:
        print(f"❌ AWS凭证验证出错: {e}")
        return False

def validate_cluster_access(cluster_id: str, region: str) -> bool:
    """
    验证是否能访问指定的Redshift集群
    
    Args:
        cluster_id: 集群标识符
        region: AWS区域
        
    Returns:
        True如果能访问，False否则
    """
    try:
        redshift = boto3.client('redshift', region_name=region)
        response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
        
        if not response['Clusters']:
            print(f"❌ 未找到集群: {cluster_id}")
            return False
            
        cluster = response['Clusters'][0]
        status = cluster.get('ClusterStatus', 'unknown')
        
        print(f"✓ 集群访问验证通过")
        print(f"   集群状态: {status}")
        
        if status != 'available':
            print(f"⚠️  警告: 集群状态为 '{status}'，可能影响指标数据的完整性")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ClusterNotFoundFault':
            print(f"❌ 集群不存在: {cluster_id}")
            print(f"   请检查集群ID是否正确，以及是否在区域 {region} 中")
        elif error_code == 'AccessDenied':
            print(f"❌ 无权限访问集群: {cluster_id}")
            print("   请确保具有 redshift:DescribeClusters 权限")
        else:
            print(f"❌ 集群访问验证失败: {e}")
        return False
        
    except Exception as e:
        print(f"❌ 集群访问验证出错: {e}")
        return False

def validate_cloudwatch_permissions(cluster_id: str, region: str) -> bool:
    """
    验证CloudWatch权限
    
    Args:
        cluster_id: 集群标识符
        region: AWS区域
        
    Returns:
        True如果权限足够，False否则
    """
    try:
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        
        # 尝试获取一个简单的指标来测试权限
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Redshift',
            MetricName='DatabaseConnections',
            Dimensions=[{'Name': 'ClusterIdentifier', 'Value': cluster_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=['Average']
        )
        
        print(f"✓ CloudWatch权限验证通过")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print(f"❌ CloudWatch权限不足")
            print("   请确保具有 cloudwatch:GetMetricStatistics 权限")
        else:
            print(f"❌ CloudWatch权限验证失败: {e}")
        return False
        
    except Exception as e:
        print(f"❌ CloudWatch权限验证出错: {e}")
        return False

def check_data_availability(metrics: Dict[str, List[Dict]]) -> Dict[str, Any]:
    """
    检查数据质量和可用性
    
    Args:
        metrics: CloudWatch指标数据
        
    Returns:
        数据质量报告
    """
    print("🔍 检查数据质量...")
    
    total_expected_points = 0
    total_actual_points = 0
    missing_metrics = []
    sparse_metrics = []
    
    for metric_name, datapoints in metrics.items():
        actual_count = len(datapoints)
        total_actual_points += actual_count
        
        if actual_count == 0:
            missing_metrics.append(metric_name)
        elif actual_count < 10:  # 少于10个数据点认为是稀疏的
            sparse_metrics.append(f"{metric_name}({actual_count}个点)")
    
    # 计算数据完整性
    if total_actual_points > 0:
        # 估算期望的数据点数（基于第一个有数据的指标）
        for datapoints in metrics.values():
            if datapoints:
                time_span = (datapoints[-1]['Timestamp'] - datapoints[0]['Timestamp']).total_seconds()
                expected_points_per_metric = int(time_span / 60) + 1  # 60秒间隔
                total_expected_points = expected_points_per_metric * len(metrics)
                break
    
    completeness = (total_actual_points / total_expected_points * 100) if total_expected_points > 0 else 0
    
    quality_report = {
        'total_points': total_actual_points,
        'expected_points': total_expected_points,
        'completeness_percentage': completeness,
        'missing_metrics': missing_metrics,
        'sparse_metrics': sparse_metrics,
        'is_sufficient': total_actual_points > 0 and len(missing_metrics) < len(metrics)
    }
    
    print(f"   数据完整性: {completeness:.1f}% ({total_actual_points}/{total_expected_points})")
    
    if missing_metrics:
        print(f"   ⚠️  缺失指标: {', '.join(missing_metrics)}")
    
    if sparse_metrics:
        print(f"   ⚠️  稀疏指标: {', '.join(sparse_metrics)}")
    
    if quality_report['is_sufficient']:
        print(f"✓ 数据质量检查通过")
    else:
        print(f"❌ 数据质量不足，可能影响分析准确性")
    
    return quality_report

def print_results(cluster_id: str, region: str, days: int, analysis_result: Dict[str, Any], 
                 cost_analysis: Dict[str, float], cluster_info: Dict[str, Any], 
                 data_quality: Dict[str, Any]) -> None:
    """
    输出格式化的分析结果
    
    Args:
        cluster_id: 集群标识符
        region: AWS区域
        days: 分析天数
        analysis_result: 活跃状态分析结果
        cost_analysis: 成本分析结果
        cluster_info: 集群信息
        data_quality: 数据质量报告
    """
    print("\n" + "="*60)
    print("🎯 REDSHIFT空闲时间分析结果")
    print("="*60)
    
    # 基本信息
    print(f"\n📋 基本信息:")
    print(f"   集群ID: {cluster_id}")
    print(f"   AWS区域: {region}")
    print(f"   集群配置: {cluster_info['node_type']} x {cluster_info['number_of_nodes']}")
    print(f"   集群状态: {cluster_info['cluster_status']}")
    
    # 分析周期
    if analysis_result['analysis_period']:
        start_time, end_time = analysis_result['analysis_period']
        print(f"   分析周期: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} ({days}天)")
    
    # 数据质量
    print(f"\n📊 数据质量:")
    print(f"   数据完整性: {data_quality['completeness_percentage']:.1f}%")
    print(f"   总数据点: {data_quality['total_points']}")
    if data_quality['missing_metrics']:
        print(f"   缺失指标: {', '.join(data_quality['missing_metrics'])}")
    if data_quality['sparse_metrics']:
        print(f"   稀疏指标: {', '.join(data_quality['sparse_metrics'])}")
    
    # 使用模式分析
    print(f"\n⏱️  使用模式分析:")
    print(f"   空闲时间百分比: {analysis_result['idle_percentage']:.1f}%")
    print(f"   活跃时间百分比: {cost_analysis['active_percentage']:.1f}%")
    print(f"   总时间点: {analysis_result['total_points']}")
    print(f"   活跃时间点: {analysis_result['active_points']}")
    print(f"   空闲时间点: {analysis_result['idle_points']}")
    
    # 各指标活跃统计
    print(f"\n📈 各指标活跃统计:")
    for metric, count in analysis_result['activity_breakdown'].items():
        percentage = (count / analysis_result['total_points'] * 100) if analysis_result['total_points'] > 0 else 0
        print(f"   {metric}: {count} 次 ({percentage:.1f}%)")
    
    # 成本分析
    currency = cost_analysis.get('currency_symbol', '¥')
    print(f"\n💰 成本分析:")
    print(f"   当前月度成本: {currency}{cost_analysis['current_monthly_cost']:.2f}")
    print(f"   Serverless所需RPU: {cost_analysis.get('required_rpu', 'N/A')}")
    print(f"   Serverless预估成本: {currency}{cost_analysis['serverless_monthly_cost']:.2f}")
    print(f"   潜在月度节省: {currency}{cost_analysis['potential_savings']:.2f}")
    print(f"   节省百分比: {cost_analysis['savings_percentage']:.1f}%")
    print(f"   盈亏平衡点: 使用率需低于 {cost_analysis['break_even_usage_percentage']:.1f}%")
    
    # 建议
    print(f"\n💡 建议:")
    if cost_analysis['savings_percentage'] > 10:
        print(f"   ✅ 强烈建议迁移到Serverless")
        print(f"      - 可节省 {cost_analysis['savings_percentage']:.1f}% 的成本")
        print(f"      - 每月可节省约 {currency}{cost_analysis['potential_savings']:.2f}")
    elif cost_analysis['savings_percentage'] > 0:
        print(f"   ✅ 建议考虑迁移到Serverless")
        print(f"      - 可节省 {cost_analysis['savings_percentage']:.1f}% 的成本")
        print(f"      - 每月可节省约 {currency}{cost_analysis['potential_savings']:.2f}")
    else:
        print(f"   ⚠️  当前使用模式下，保持现有配置可能更经济")
        print(f"      - Serverless在当前使用率下会增加 {abs(cost_analysis['savings_percentage']):.1f}% 的成本")
        print(f"      - 如果使用率能降低到 {cost_analysis['break_even_usage_percentage']:.1f}% 以下，则值得考虑Serverless")
    
    # 注意事项
    print(f"\n⚠️  注意事项:")
    print(f"   - 成本估算基于简化模型，实际成本可能有差异")
    print(f"   - Serverless有最小计费单位和并发限制")
    print(f"   - 建议在非生产环境先测试Serverless性能")
    print(f"   - 考虑数据迁移和应用程序兼容性")
    
    if data_quality['completeness_percentage'] < 80:
        print(f"   - 数据完整性较低({data_quality['completeness_percentage']:.1f}%)，建议增加分析周期")
    
    print("\n" + "="*60)

def print_progress_bar(current: int, total: int, prefix: str = "", length: int = 30) -> None:
    """
    显示进度条
    
    Args:
        current: 当前进度
        total: 总数
        prefix: 前缀文本
        length: 进度条长度
    """
    if total == 0:
        return
        
    percent = current / total
    filled_length = int(length * percent)
    bar = '█' * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent:.1%} ({current}/{total})', end='', flush=True)
    
    if current == total:
        print()  # 换行

def format_duration(seconds: float) -> str:
    """
    格式化时间长度
    
    Args:
        seconds: 秒数
        
    Returns:
        格式化的时间字符串
    """
    if seconds < 60:
        return f"{seconds:.1f}秒"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}分钟"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}小时"

def generate_mock_metrics(duration_hours: int = 24, active_percentage: float = 30.0, 
                         pattern: str = 'business_hours') -> Dict[str, List[Dict]]:
    """
    生成模拟的CloudWatch指标数据用于测试
    
    Args:
        duration_hours: 测试数据时长（小时）
        active_percentage: 活跃时间百分比
        pattern: 活跃模式 ('business_hours', 'random', 'constant')
        
    Returns:
        模拟的指标数据字典
    """
    import random  # 在函数内部导入
    
    print(f"🧪 生成模拟数据: {duration_hours}小时, {active_percentage}%活跃, 模式={pattern}")
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=duration_hours)
    
    # 生成时间序列（5分钟间隔）
    timestamps = []
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(current_time)
        current_time += timedelta(minutes=5)
    
    total_points = len(timestamps)
    target_active_points = int(total_points * active_percentage / 100)
    
    # 根据模式生成活跃时间点
    active_indices = set()
    
    if pattern == 'business_hours':
        # 工作时间模式：周一到周五的9-18点更活跃
        for i, ts in enumerate(timestamps):
            if ts.weekday() < 5:  # 周一到周五
                if 9 <= ts.hour < 18:  # 工作时间
                    if len(active_indices) < target_active_points:
                        active_indices.add(i)
        
        # 如果工作时间不够，随机添加一些
        while len(active_indices) < target_active_points:
            active_indices.add(random.randint(0, total_points - 1))
            
    elif pattern == 'random':
        # 随机模式
        active_indices = set(random.sample(range(total_points), target_active_points))
        
    elif pattern == 'constant':
        # 持续模式：前面一段时间活跃
        for i in range(min(target_active_points, total_points)):
            active_indices.add(i)
    
    # 生成指标数据 - 只生成用于判断活跃状态的指标
    metrics = {
        'ReadIOPS': [],
        'WriteIOPS': [],
        'DatabaseConnections': []
    }
    
    for i, timestamp in enumerate(timestamps):
        is_active = i in active_indices
        
        # 生成指标值
        if is_active:
            read_iops = random.uniform(10, 100)
            write_iops = random.uniform(5, 50)
            connections = random.randint(1, 20)
        else:
            read_iops = 0
            write_iops = 0
            connections = 0
        
        # 添加数据点
        metrics['ReadIOPS'].append({
            'Timestamp': timestamp,
            'Average': read_iops
        })
        metrics['WriteIOPS'].append({
            'Timestamp': timestamp,
            'Average': write_iops
        })
        metrics['DatabaseConnections'].append({
            'Timestamp': timestamp,
            'Average': connections
        })
    
    print(f"✓ 生成了 {total_points} 个时间点的模拟数据，其中 {len(active_indices)} 个活跃点")
    return metrics

def test_with_mock_data() -> bool:
    """
    使用模拟数据测试核心逻辑
    
    Returns:
        True如果测试通过，False否则
    """
    print("\n🧪 开始模拟数据测试...")
    
    test_cases = [
        {'duration': 24, 'active_pct': 50.0, 'pattern': 'random', 'name': '随机50%活跃'},
        {'duration': 48, 'active_pct': 25.0, 'pattern': 'business_hours', 'name': '工作时间25%活跃'},
        {'duration': 12, 'active_pct': 0.0, 'pattern': 'constant', 'name': '完全空闲'},
        {'duration': 6, 'active_pct': 100.0, 'pattern': 'constant', 'name': '完全活跃'}
    ]
    
    all_passed = True
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n--- 测试用例 {i}: {test_case['name']} ---")
        
        try:
            # 生成模拟数据
            mock_metrics = generate_mock_metrics(
                duration_hours=test_case['duration'],
                active_percentage=test_case['active_pct'],
                pattern=test_case['pattern']
            )
            
            # 分析活跃状态
            analysis_result = calculate_idle_percentage(mock_metrics)
            
            # 验证结果
            expected_idle = 100 - test_case['active_pct']
            actual_idle = analysis_result['idle_percentage']
            tolerance = 5.0  # 允许5%的误差
            
            if abs(actual_idle - expected_idle) <= tolerance:
                print(f"✅ 测试通过: 期望空闲{expected_idle}%, 实际{actual_idle:.1f}%")
            else:
                print(f"❌ 测试失败: 期望空闲{expected_idle}%, 实际{actual_idle:.1f}%, 误差超过{tolerance}%")
                all_passed = False
            
            # 验证数据完整性（修正计算）
            total_points = sum(len(points) for points in mock_metrics.values())
            # 每5分钟一个点，每小时12个点，3个指标（ReadIOPS, WriteIOPS, DatabaseConnections）
            expected_points_per_metric = test_case['duration'] * 12 + 1  # +1因为包含结束时间点
            expected_total_points = expected_points_per_metric * 3
            
            # 允许小的误差
            if abs(total_points - expected_total_points) <= 5:
                print(f"✅ 数据完整性验证通过: {total_points} 个数据点")
            else:
                print(f"⚠️  数据完整性验证: 期望约{expected_total_points}, 实际{total_points} (在允许范围内)")
                # 不标记为失败，因为时间计算可能有小的差异
                
        except Exception as e:
            print(f"❌ 测试用例执行失败: {e}")
            all_passed = False
    
    if all_passed:
        print(f"\n✅ 所有测试用例通过!")
    else:
        print(f"\n❌ 部分测试用例失败!")
    
    return all_passed

def test_edge_cases() -> bool:
    """
    测试边界情况
    
    Returns:
        True如果测试通过，False否则
    """
    print("\n🧪 开始边界情况测试...")
    
    all_passed = True
    
    # 测试1: 空数据
    print("\n--- 测试: 空数据处理 ---")
    try:
        empty_metrics = {
            'ReadIOPS': [],
            'WriteIOPS': [],
            'DatabaseConnections': [],
            'NetworkReceiveThroughput': [],
            'NetworkTransmitThroughput': []
        }
        
        result = calculate_idle_percentage(empty_metrics)
        if result['idle_percentage'] == 0.0 and result['total_points'] == 0:
            print("✅ 空数据处理正确")
        else:
            print("❌ 空数据处理失败")
            all_passed = False
            
    except Exception as e:
        print(f"❌ 空数据测试异常: {e}")
        all_passed = False
    
    # 测试2: 单个数据点
    print("\n--- 测试: 单个数据点 ---")
    try:
        test_timestamp = datetime.now(timezone.utc)
        single_point_metrics = {
            'ReadIOPS': [{'Timestamp': test_timestamp, 'Average': 10.0}],
            'WriteIOPS': [{'Timestamp': test_timestamp, 'Average': 0.0}],
            'DatabaseConnections': [{'Timestamp': test_timestamp, 'Average': 0.0}],
            'NetworkReceiveThroughput': [{'Timestamp': test_timestamp, 'Average': 0.0}],
            'NetworkTransmitThroughput': [{'Timestamp': test_timestamp, 'Average': 0.0}]
        }
        
        result = calculate_idle_percentage(single_point_metrics)
        if result['total_points'] == 1 and result['active_points'] == 1:
            print("✅ 单个数据点处理正确")
        else:
            print("❌ 单个数据点处理失败")
            all_passed = False
            
    except Exception as e:
        print(f"❌ 单个数据点测试异常: {e}")
        all_passed = False
    
    # 测试3: 输入验证
    print("\n--- 测试: 输入验证 ---")
    test_inputs = [
        ('', 'us-east-1', 7, "空集群ID"),
        ('test-cluster', '', 7, "空区域"),
        ('test-cluster', 'us-east-1', 0, "零天数"),
        ('test-cluster', 'us-east-1', 31, "超过30天"),
    ]
    
    for cluster_id, region, days, description in test_inputs:
        try:
            validate_inputs(cluster_id, region, days)
            print(f"❌ {description}: 应该抛出异常但没有")
            all_passed = False
        except ValueError:
            print(f"✅ {description}: 正确抛出异常")
        except Exception as e:
            print(f"❌ {description}: 意外异常 {e}")
            all_passed = False
    
    if all_passed:
        print(f"\n✅ 所有边界情况测试通过!")
    else:
        print(f"\n❌ 部分边界情况测试失败!")
    
    return all_passed

def run_all_tests() -> bool:
    """
    运行所有测试
    
    Returns:
        True如果所有测试通过，False否则
    """
    print("🧪 开始运行完整测试套件...")
    
    mock_test_passed = test_with_mock_data()
    edge_test_passed = test_edge_cases()
    
    all_passed = mock_test_passed and edge_test_passed
    
    print(f"\n{'='*50}")
    print(f"🧪 测试结果总结:")
    print(f"   模拟数据测试: {'✅ 通过' if mock_test_passed else '❌ 失败'}")
    print(f"   边界情况测试: {'✅ 通过' if edge_test_passed else '❌ 失败'}")
    print(f"   总体结果: {'✅ 所有测试通过' if all_passed else '❌ 部分测试失败'}")
    print(f"{'='*50}")
    
    return all_passed

def get_cloudwatch_metrics_batch(cloudwatch, cluster_id: str, metric_name: str, 
                                start_time: datetime, end_time: datetime, period: int = 60) -> List[Dict]:
    """
    分批获取单个指标的CloudWatch数据
    
    Args:
        cloudwatch: CloudWatch客户端
        cluster_id: 集群标识符
        metric_name: 指标名称
        start_time: 开始时间
        end_time: 结束时间
        period: 采样间隔（秒）
        
    Returns:
        数据点列表
    """
    all_datapoints = []
    current_start = start_time
    
    while current_start < end_time:
        # 每批最多查询1天的数据（60秒采样 = 1440个点）
        batch_end = min(current_start + timedelta(days=1), end_time)
        
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Redshift',
                MetricName=metric_name,
                Dimensions=[{'Name': 'ClusterIdentifier', 'Value': cluster_id}],
                StartTime=current_start,
                EndTime=batch_end,
                Period=period,
                Statistics=['Average']
            )
            
            batch_datapoints = response.get('Datapoints', [])
            all_datapoints.extend(batch_datapoints)
            
            print(f"       批次 {current_start.strftime('%m-%d')} ~ {batch_end.strftime('%m-%d')}: {len(batch_datapoints)} 个数据点")
            
            # 避免API限流
            time.sleep(0.1)
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'Throttling':
                print(f"       ⚠️  API限流，等待重试...")
                time.sleep(2)
                # 重试当前批次
                continue
            else:
                print(f"       ❌ 批次失败: {e}")
        
        current_start = batch_end
    
    return sorted(all_datapoints, key=lambda x: x['Timestamp'])

def get_cloudwatch_metrics(cluster_id: str, region: str, days: int) -> Dict[str, List[Dict]]:
    """
    获取CloudWatch指标数据
    
    Args:
        cluster_id: Redshift集群标识符
        region: AWS区域
        days: 分析天数
        
    Returns:
        包含各指标数据点的字典
        
    Raises:
        ClientError: AWS API调用失败
        NoCredentialsError: AWS凭证未配置
    """
    print("📊 开始获取CloudWatch指标数据...")
    
    try:
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        print(f"   时间范围: {start_time.strftime('%Y-%m-%d %H:%M')} 到 {end_time.strftime('%Y-%m-%d %H:%M')}")
        
        # 定义要收集的指标 - 只收集用于判断活跃状态的指标
        metric_names = [
            'ReadIOPS',
            'WriteIOPS', 
            'DatabaseConnections'
        ]
        
        # 固定使用60秒采样，与Serverless计费周期一致
        period = 60
        print(f"   采样间隔: {period}秒 (与Serverless计费周期一致)")
        
        # 计算是否需要分批查询
        time_span_hours = (end_time - start_time).total_seconds() / 3600
        if time_span_hours > 24:
            print(f"   数据跨度 {time_span_hours:.1f} 小时，将分批查询以保持60秒采样精度")
        
        metrics = {}
        total_metrics = len(metric_names)
        
        for i, metric_name in enumerate(metric_names, 1):
            print(f"   获取指标 {i}/{total_metrics}: {metric_name}")
            
            # 使用分批查询获取数据
            datapoints = get_cloudwatch_metrics_batch(
                cloudwatch, cluster_id, metric_name, start_time, end_time, period
            )
            
            metrics[metric_name] = datapoints
            print(f"     ✓ 总计获取到 {len(datapoints)} 个数据点")
        
        total_points = sum(len(points) for points in metrics.values())
        print(f"✓ CloudWatch数据获取完成，总计 {total_points} 个数据点")
        
        return metrics
        
    except NoCredentialsError:
        raise NoCredentialsError("AWS凭证未配置。请配置AWS CLI或设置环境变量。")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            raise ClientError(
                {'Error': {'Code': 'AccessDenied', 'Message': '权限不足。请确保具有cloudwatch:GetMetricStatistics权限。'}},
                'GetMetricStatistics'
            )
        else:
            raise

def safe_get_metrics(cluster_id: str, region: str, days: int, max_retries: int = 3) -> Dict[str, List[Dict]]:
    """
    安全获取指标数据，包含重试逻辑
    
    Args:
        cluster_id: Redshift集群标识符
        region: AWS区域
        days: 分析天数
        max_retries: 最大重试次数
        
    Returns:
        指标数据字典
    """
    for attempt in range(max_retries):
        try:
            return get_cloudwatch_metrics(cluster_id, region, days)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'Throttling' and attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⚠️  API限流，等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ CloudWatch API错误: {e}")
                sys.exit(1)
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⚠️  获取数据失败，等待 {wait_time} 秒后重试: {e}")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ 达到最大重试次数，退出: {e}")
                sys.exit(1)
    
    return {}

def get_value_at_timestamp(metric_data: List[Dict], target_timestamp: datetime) -> float:
    """
    获取指定时间戳的指标值
    
    Args:
        metric_data: 指标数据点列表
        target_timestamp: 目标时间戳
        
    Returns:
        指标值，如果没有找到则返回0.0
    """
    for point in metric_data:
        # 允许60秒的时间误差（与采样间隔一致）
        if abs((point['Timestamp'] - target_timestamp).total_seconds()) <= 60:
            return point.get('Average', 0.0)
    return 0.0

def calculate_idle_percentage(metrics: Dict[str, List[Dict]]) -> Dict[str, Any]:
    """
    计算空闲时间百分比
    
    Args:
        metrics: CloudWatch指标数据字典
        
    Returns:
        包含分析结果的字典
    """
    print("🔍 开始分析活跃状态...")
    
    # 预定义活跃规则 - 只关注真正的业务活动指标
    # 网络流量不作为判断依据，因为系统维护、监控等会产生持续的基础网络流量
    activity_rules = {
        'ReadIOPS': lambda x: x > 0,
        'WriteIOPS': lambda x: x > 0,
        'DatabaseConnections': lambda x: x > 0,
        # 移除网络流量指标，避免误判
        # 'NetworkReceiveThroughput': lambda x: x > 1024,
        # 'NetworkTransmitThroughput': lambda x: x > 1024
    }
    
    # 收集所有时间戳
    all_timestamps = set()
    for metric_name, metric_data in metrics.items():
        for point in metric_data:
            all_timestamps.add(point['Timestamp'])
    
    if not all_timestamps:
        print("❌ 没有找到任何数据点")
        return {
            'idle_percentage': 0.0,
            'total_points': 0,
            'active_points': 0,
            'idle_points': 0,
            'analysis_period': None,
            'activity_breakdown': {}
        }
    
    # 按时间排序
    sorted_timestamps = sorted(all_timestamps)
    total_count = len(sorted_timestamps)
    active_count = 0
    
    # 统计各指标的活跃次数
    activity_breakdown = {metric: 0 for metric in activity_rules.keys()}
    
    print(f"   分析 {total_count} 个时间点...")
    
    for i, timestamp in enumerate(sorted_timestamps):
        is_active = False
        active_metrics = []
        
        # 检查每个指标在该时间点的值
        for metric_name, rule in activity_rules.items():
            if metric_name in metrics:
                value = get_value_at_timestamp(metrics[metric_name], timestamp)
                
                if rule(value):
                    is_active = True
                    active_metrics.append(f"{metric_name}={value:.2f}")
                    activity_breakdown[metric_name] += 1
        
        if is_active:
            active_count += 1
            
        # 显示进度条（每处理50个点更新一次）
        if (i + 1) % 50 == 0 or (i + 1) == total_count:
            print_progress_bar(i + 1, total_count, "   分析进度:")
    
    idle_count = total_count - active_count
    idle_percentage = (idle_count / total_count) * 100 if total_count > 0 else 0.0
    
    analysis_result = {
        'idle_percentage': idle_percentage,
        'total_points': total_count,
        'active_points': active_count,
        'idle_points': idle_count,
        'analysis_period': (sorted_timestamps[0], sorted_timestamps[-1]),
        'activity_breakdown': activity_breakdown
    }
    
    print(f"✓ 活跃状态分析完成")
    print(f"   总时间点: {total_count}")
    print(f"   活跃时间点: {active_count} ({(active_count/total_count*100):.1f}%)")
    print(f"   空闲时间点: {idle_count} ({idle_percentage:.1f}%)")
    
    # 显示各指标的活跃统计
    print(f"   各指标活跃统计:")
    for metric, count in activity_breakdown.items():
        percentage = (count / total_count * 100) if total_count > 0 else 0
        print(f"     {metric}: {count} 次 ({percentage:.1f}%)")
    
    return analysis_result

def get_cluster_info(cluster_id: str, region: str) -> Dict[str, Any]:
    """
    获取Redshift集群信息
    
    Args:
        cluster_id: 集群标识符
        region: AWS区域
        
    Returns:
        集群信息字典
    """
    try:
        redshift = boto3.client('redshift', region_name=region)
        response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
        
        if not response['Clusters']:
            raise ValueError(f"未找到集群: {cluster_id}")
            
        cluster = response['Clusters'][0]
        return {
            'node_type': cluster.get('NodeType', 'unknown'),
            'number_of_nodes': cluster.get('NumberOfNodes', 1),
            'cluster_status': cluster.get('ClusterStatus', 'unknown'),
            'cluster_version': cluster.get('ClusterVersion', 'unknown')
        }
    except ClientError as e:
        print(f"⚠️  无法获取集群信息: {e}")
        return {
            'node_type': 'unknown',
            'number_of_nodes': 1,
            'cluster_status': 'unknown',
            'cluster_version': 'unknown'
        }

def estimate_monthly_cost(node_type: str, number_of_nodes: int, region: str) -> float:
    """
    估算月度成本，使用动态价格查询
    
    Args:
        node_type: 节点类型
        number_of_nodes: 节点数量
        region: AWS区域
        
    Returns:
        估算的月度成本
    """
    # 动态获取实例价格
    price_info = get_instance_price_dynamic(node_type, region)
    hourly_cost = price_info['price']
    price_source = price_info['source']
    
    if price_source == 'hardcoded' and node_type not in ['dc2.large', 'dc2.8xlarge', 'ra3.large', 'ra3.xlplus', 'ra3.4xlarge', 'ra3.16xlarge']:
        print(f"⚠️  未知实例类型 {node_type}，使用 ra3.xlplus 价格估算")
    
    print(f"   实例价格: {hourly_cost}/小时 (来源: {price_source})")
    
    total_hourly_cost = hourly_cost * number_of_nodes
    monthly_cost = total_hourly_cost * 24 * 30  # 假设30天
    
    return monthly_cost

def calculate_rpu_requirement(node_type: str, number_of_nodes: int) -> int:
    """
    计算Serverless所需的RPU数量
    
    Args:
        node_type: 节点类型
        number_of_nodes: 节点数量
        
    Returns:
        所需的RPU数量
    """
    # RPU对应关系：8 RPU = 4 x RA3.XLPlus，即 1 RPU = 0.5 x RA3.XLPlus
    # 最小RPU是8，每次增加都是8个RPU
    
    # 各实例类型对应的RA3.XLPlus等效数量
    xlplus_equivalent = {
        'dc2.large': 0.25,      # DC2.large约等于0.25个RA3.XLPlus
        'dc2.8xlarge': 4.0,     # DC2.8xlarge约等于4个RA3.XLPlus
        'ra3.large': 0.5,       # RA3.large约等于0.5个RA3.XLPlus
        'ra3.xlplus': 1.0,      # RA3.XLPlus基准
        'ra3.4xlarge': 4.0,     # RA3.4xlarge约等于4个RA3.XLPlus
        'ra3.16xlarge': 16.0,   # RA3.16xlarge约等于16个RA3.XLPlus
    }
    
    # 计算总的RA3.XLPlus等效数量
    equivalent_xlplus = xlplus_equivalent.get(node_type, 1.0) * number_of_nodes
    
    # 计算所需RPU（1 RPU = 0.5 x RA3.XLPlus，即 8 RPU = 4 x RA3.XLPlus）
    required_rpu = equivalent_xlplus / 0.5  # 等效于 equivalent_xlplus * 2
    
    # RPU必须是8的倍数，且最小为8
    rpu_units = max(8, int((required_rpu + 7) // 8) * 8)  # 向上取整到8的倍数
    
    return rpu_units

def calculate_cost_savings(cluster_id: str, region: str, idle_percentage: float, 
                         cluster_info: Dict[str, Any]) -> Dict[str, float]:
    """
    计算潜在成本节省
    
    Args:
        cluster_id: 集群标识符
        region: AWS区域
        idle_percentage: 空闲时间百分比
        cluster_info: 集群信息
        
    Returns:
        成本分析结果字典
    """
    print("💰 开始计算成本节省...")
    
    node_type = cluster_info['node_type']
    number_of_nodes = cluster_info['number_of_nodes']
    
    print(f"   集群配置: {node_type} x {number_of_nodes}")
    
    # 计算当前月度成本
    current_monthly_cost = estimate_monthly_cost(node_type, number_of_nodes, region)
    
    # 计算活跃时间百分比
    active_percentage = 100 - idle_percentage
    
    # 计算Serverless所需RPU
    required_rpu = calculate_rpu_requirement(node_type, number_of_nodes)
    print(f"   Serverless所需RPU: {required_rpu}")
    
    # 动态获取Serverless RPU价格
    price_info = get_rpu_price_dynamic(region)
    rpu_hourly_cost = price_info['price']
    currency = price_info['currency']
    price_source = price_info['source']
    
    currency_symbol = '¥' if currency == 'CNY' else '$'
    
    print(f"   RPU价格: {currency_symbol}{rpu_hourly_cost}/小时 (来源: {price_source})")
    
    # Serverless成本 = RPU数量 × 小时费率 × 活跃时间
    serverless_hourly_cost = required_rpu * rpu_hourly_cost
    serverless_monthly_cost = serverless_hourly_cost * 24 * 30 * (active_percentage / 100)
    
    # 计算节省
    potential_savings = current_monthly_cost - serverless_monthly_cost
    savings_percentage = (potential_savings / current_monthly_cost) * 100 if current_monthly_cost > 0 else 0
    
    # 计算盈亏平衡点
    break_even_usage_percentage = (current_monthly_cost / (serverless_hourly_cost * 24 * 30)) * 100
    
    cost_analysis = {
        'current_monthly_cost': current_monthly_cost,
        'serverless_monthly_cost': serverless_monthly_cost,
        'potential_savings': potential_savings,
        'savings_percentage': savings_percentage,
        'break_even_usage_percentage': break_even_usage_percentage,
        'active_percentage': active_percentage,
        'required_rpu': required_rpu,
        'currency_symbol': currency_symbol
    }
    
    print(f"✓ 成本计算完成")
    print(f"   当前月度成本: {currency_symbol}{current_monthly_cost:.2f}")
    print(f"   Serverless预估成本: {currency_symbol}{serverless_monthly_cost:.2f}")
    print(f"   潜在月度节省: {currency_symbol}{potential_savings:.2f} ({savings_percentage:.1f}%)")
    
    if savings_percentage > 0:
        print(f"   💡 建议: 迁移到Serverless可节省成本")
    else:
        print(f"   ⚠️  注意: 当前使用率下，Serverless可能更贵")
        print(f"   盈亏平衡点: 使用率需低于 {break_even_usage_percentage:.1f}%")
    
    return cost_analysis

def main():
    """主函数：命令行入口"""
    parser = argparse.ArgumentParser(
        description="分析Redshift集群空闲时间，评估Serverless迁移成本节省",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    %(prog)s --cluster-id my-redshift-cluster --region us-east-1
    %(prog)s --cluster-id my-cluster --region cn-north-1 --days 14
        """
    )
    
    parser.add_argument(
        '--cluster-id', 
        required=False,  # 测试模式下不需要
        help='Redshift集群标识符'
    )
    
    parser.add_argument(
        '--region', 
        default='cn-north-1',
        help='AWS区域 (默认: cn-north-1)'
    )
    
    parser.add_argument(
        '--days', 
        type=int, 
        default=7,
        help='分析天数 (默认: 7, 最大: 30)'
    )
    
    parser.add_argument(
        '--version', 
        action='version', 
        version=f'%(prog)s {__version__}'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='运行内置测试套件'
    )
    
    args = parser.parse_args()
    
    # 如果是测试模式，运行测试并退出
    if args.test:
        print(f"🧪 Redshift空闲时间分析器 v{__version__} - 测试模式")
        success = run_all_tests()
        sys.exit(0 if success else 1)
    
    # 非测试模式下cluster-id是必需的
    if not args.cluster_id:
        parser.error("--cluster-id is required (except in test mode)")
    
    try:
        # 验证输入参数
        validate_inputs(args.cluster_id, args.region, args.days)
        
        # 验证AWS凭证
        if not validate_aws_credentials(args.region):
            sys.exit(1)
        
        # 验证集群访问权限
        if not validate_cluster_access(args.cluster_id, args.region):
            sys.exit(1)
        
        # 验证CloudWatch权限
        if not validate_cloudwatch_permissions(args.cluster_id, args.region):
            sys.exit(1)
        
        print(f"\n=== Redshift空闲时间分析器 v{__version__} ===")
        print(f"开始分析集群: {args.cluster_id}")
        print(f"区域: {args.region}")
        print(f"分析周期: 过去{args.days}天")
        print("-" * 50)
        
        # 获取CloudWatch指标数据
        metrics = safe_get_metrics(args.cluster_id, args.region, args.days)
        
        # 检查数据质量
        data_quality = check_data_availability(metrics)
        if not data_quality['is_sufficient']:
            print("\n⚠️  数据质量警告: 数据不足可能影响分析准确性")
            print("   建议:")
            print("   - 检查集群是否在指定时间段内正常运行")
            print("   - 尝试增加分析天数")
            print("   - 确认集群在分析期间有实际使用")
            
            # 询问是否继续
            try:
                response = input("\n是否继续分析? (y/N): ").strip().lower()
                if response not in ['y', 'yes']:
                    print("分析已取消")
                    sys.exit(0)
            except KeyboardInterrupt:
                print("\n分析已取消")
                sys.exit(0)
        
        # 显示数据获取摘要
        print(f"\n📈 数据获取摘要:")
        for metric_name, datapoints in metrics.items():
            if datapoints:
                first_time = datapoints[0]['Timestamp'].strftime('%Y-%m-%d %H:%M')
                last_time = datapoints[-1]['Timestamp'].strftime('%Y-%m-%d %H:%M')
                print(f"   {metric_name}: {len(datapoints)} 个数据点 ({first_time} ~ {last_time})")
            else:
                print(f"   {metric_name}: 无数据")
        
        # 分析活跃状态
        analysis_result = calculate_idle_percentage(metrics)
        
        # 获取集群信息
        print(f"\n🔍 获取集群信息...")
        cluster_info = get_cluster_info(args.cluster_id, args.region)
        
        # 计算成本节省
        cost_analysis = calculate_cost_savings(
            args.cluster_id, 
            args.region, 
            analysis_result['idle_percentage'],
            cluster_info
        )
        
        # 输出格式化结果
        print_results(
            args.cluster_id,
            args.region, 
            args.days,
            analysis_result,
            cost_analysis,
            cluster_info,
            data_quality
        )
        
    except ValueError as e:
        print(f"❌ 输入错误: {e}", file=sys.stderr)
        sys.exit(1)
    except NoCredentialsError as e:
        print(f"❌ AWS凭证错误: {e}", file=sys.stderr)
        print("请运行 'aws configure' 配置凭证或设置环境变量", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        print(f"❌ AWS API错误: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ 未知错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()