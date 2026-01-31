#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CRAG快速测试脚本
用于快速测试和对比RAG、CRAG和No-Retrieval方法
"""

import argparse
import os
import sys
from pathlib import Path

# 添加父目录到路径
sys.path.append(str(Path(__file__).parent))

from CRAG_Inference import main as run_inference
import subprocess


def run_comparison(args):
    """运行对比实验"""
    
    base_args = {
        'generator_path': args.generator_path,
        'evaluator_path': args.evaluator_path,
        'input_file': args.input_file,
        'task': args.task,
        'device': args.device,
        'ndocs': args.ndocs,
        'batch_size': args.batch_size,
        'upper_threshold': args.upper_threshold,
        'lower_threshold': args.lower_threshold,
    }
    
    methods = ['rag', 'crag', 'no_retrieval']
    results = {}
    
    print("=" * 60)
    print("开始对比实验：RAG vs CRAG vs No-Retrieval")
    print("=" * 60)
    
    for method in methods:
        print(f"\n正在运行: {method.upper()}")
        print("-" * 60)
        
        output_file = args.output_dir / f"{method}_output.txt"
        
        # 构建参数
        cmd_args = [
            'python', 'CRAG_Inference.py',
            '--generator_path', base_args['generator_path'],
            '--evaluator_path', base_args['evaluator_path'],
            '--input_file', base_args['input_file'],
            '--output_file', str(output_file),
            '--task', base_args['task'],
            '--method', method,
            '--device', base_args['device'],
            '--ndocs', str(base_args['ndocs']),
            '--batch_size', str(base_args['batch_size']),
        ]
        
        # 只有CRAG需要知识路径
        if method == 'crag':
            cmd_args.extend([
                '--internal_knowledge_path', args.internal_knowledge_path,
                '--external_knowledge_path', args.external_knowledge_path,
                '--combined_knowledge_path', args.combined_knowledge_path,
                '--upper_threshold', str(base_args['upper_threshold']),
                '--lower_threshold', str(base_args['lower_threshold']),
            ])
        
        # 运行推理
        try:
            result = subprocess.run(
                cmd_args,
                cwd=Path(__file__).parent,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"✓ {method.upper()} 运行成功")
                results[method] = output_file
            else:
                print(f"✗ {method.upper()} 运行失败:")
                print(result.stderr)
                
        except Exception as e:
            print(f"✗ {method.upper()} 运行出错: {e}")
    
    # 显示结果文件位置
    print("\n" + "=" * 60)
    print("实验结果文件:")
    print("=" * 60)
    for method, output_file in results.items():
        if output_file.exists():
            print(f"{method.upper()}: {output_file}")
            # 显示前几行结果
            with open(output_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:5]
                print(f"  前5个结果:")
                for i, line in enumerate(lines, 1):
                    print(f"    {i}. {line.strip()[:80]}...")
        else:
            print(f"{method.upper()}: 文件不存在")
    
    print("\n提示: 使用 eval.py 脚本评估结果质量")


def check_environment():
    """检查运行环境"""
    print("检查运行环境...")
    
    checks = {
        'Python版本': sys.version,
        'CUDA可用': False,
        '必要文件': []
    }
    
    # 检查CUDA
    try:
        import torch
        checks['CUDA可用'] = torch.cuda.is_available()
        if checks['CUDA可用']:
            checks['CUDA设备数'] = torch.cuda.device_count()
    except:
        pass
    
    # 检查必要模块
    try:
        import transformers
        checks['transformers已安装'] = True
    except:
        checks['transformers已安装'] = False
    
    try:
        from vllm import LLM
        checks['vllm已安装'] = True
    except:
        checks['vllm已安装'] = False
    
    print("\n环境检查结果:")
    for key, value in checks.items():
        print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(description='CRAG快速测试工具')
    
    # 基础参数
    parser.add_argument('--generator_path', type=str, required=True,
                        help='生成器模型路径')
    parser.add_argument('--evaluator_path', type=str, required=True,
                        help='评估器模型路径')
    parser.add_argument('--input_file', type=str, required=True,
                        help='输入文件路径')
    parser.add_argument('--task', type=str, default='popqa',
                        help='任务类型')
    parser.add_argument('--device', type=str, default='cuda:0',
                        help='设备')
    parser.add_argument('--ndocs', type=int, default=10,
                        help='文档数量')
    parser.add_argument('--batch_size', type=int, default=8,
                        help='批次大小')
    
    # CRAG特定参数
    parser.add_argument('--internal_knowledge_path', type=str,
                        help='内部知识路径（CRAG需要）')
    parser.add_argument('--external_knowledge_path', type=str,
                        help='外部知识路径（CRAG需要）')
    parser.add_argument('--combined_knowledge_path', type=str,
                        help='组合知识路径（CRAG需要）')
    parser.add_argument('--upper_threshold', type=float, default=0.592,
                        help='上阈值')
    parser.add_argument('--lower_threshold', type=float, default=0.995,
                        help='下阈值')
    
    # 输出参数
    parser.add_argument('--output_dir', type=str, default='../data/popqa/output',
                        help='输出目录')
    
    # 功能选择
    parser.add_argument('--check_env', action='store_true',
                        help='检查运行环境')
    parser.add_argument('--compare', action='store_true',
                        help='运行对比实验')
    
    args = parser.parse_args()
    
    # 转换路径为Path对象
    args.output_dir = Path(args.output_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.check_env:
        check_environment()
    elif args.compare:
        run_comparison(args)
    else:
        parser.print_help()
        print("\n请使用 --check_env 检查环境或 --compare 运行对比实验")


if __name__ == '__main__':
    main()

