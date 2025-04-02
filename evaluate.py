import json
import logging
from typing import Dict, List
from fastapi import UploadFile
from models import DataQualityReport, BusinessInsights

logger = logging.getLogger(__name__)

class Evaluator:
    def __init__(self):
        self.max_score = 100
        self.weights = {
            'data_quality': 0.7,
            'business_insights': 0.2,
            'output_format': 0.1,
            'performance': 0.0  
        }
        self.benchmarks = {  # Time benchmarks in seconds
            'basic': 30,
            'intermediate': 60,
            'advanced': 120  
        }

    async def evaluate(self, 
                      data_quality: UploadFile,
                      insights: UploadFile,
                      cleaned_data: UploadFile,
                      processing_time: float = None) -> Dict:
        """Evaluate participant submission"""
        score = 0
        details = {}
        performance = {}
        
        # Calculate performance score if time provided
        if processing_time:
            # Determine dataset tier based on size
            data_size = len(await cleaned_data.read())
            tier = 'basic' if data_size < 1000000 else 'intermediate' if data_size < 5000000 else 'advanced'
            
            # Score based on benchmark (100 for meeting benchmark, scales down)
            perf_score = max(0, 100 * (self.benchmarks[tier] / processing_time))
            performance = {
                'processing_time': processing_time,
                'benchmark': self.benchmarks[tier],
                'score': perf_score
            }
        
        # Validate data quality report
        dq_report = DataQualityReport.parse_obj(await self._parse_upload_file(data_quality))
        details['data_quality'] = self._evaluate_data_quality(dq_report)
        
        # Validate business insights
        biz_insights = BusinessInsights.parse_obj(await self._parse_upload_file(insights))
        details['business_insights'] = self._evaluate_insights(biz_insights)
        
        # Validate output formats
        details['output_format'] = self._evaluate_output_formats(
            await cleaned_data.read()
        )
        
        # Calculate weighted final score
        score = (details['data_quality']['score'] * self.weights['data_quality'] +
                details['business_insights']['score'] * self.weights['business_insights'] +
                details['output_format']['score'] * self.weights['output_format'])
        
        # Include performance score if available
        if performance:
            score += performance['score'] * self.weights['performance']
            details['performance'] = performance
            
        return {
            'status': 'success',
            'score': min(round(score, 2), 100),  # Cap at 100
            'details': details,
            'performance': performance
        }

    async def _parse_upload_file(self, file: UploadFile):
        return json.loads(await file.read())

    def _evaluate_data_quality(self, report: DataQualityReport) -> Dict:
        """Score data quality improvements (max 100)"""
        logger.info(f"Evaluating data quality: {report.dict()}")
        base_score = 60
        issues_fixed = sum(report.missing_values.values()) + \
                      sum(report.invalid_records.values()) + \
                      sum(report.schema_violations.values())
        
        logger.info(f"Found {issues_fixed} issues fixed")
        # 2 points per fixed issue, up to 40 points
        score = min(base_score + (issues_fixed * 2), 100)
        logger.info(f"Calculated data quality score: {score}")
        
        return {
            'score': score,
            'issues_fixed': issues_fixed,
            'details': report.dict()
        }

    def _evaluate_insights(self, insights: BusinessInsights) -> Dict:
        """Score business insights (max 100)"""
        logger.info(f"Evaluating insights: {insights.dict()}")
        required_metrics = [
            'top_customers',
            'top_products', 
            'shipping_performance',
            'return_analysis'
        ]
        
        if not all(metric in insights.dict() for metric in required_metrics):
            logger.warning("Missing required metrics")
            return {
                'score': 0,
                'details': insights.dict()
            }
            
        # Base score for having all metrics (full points since we control the problem)
        score = 100
        logger.info(f"Base insights score: {score}")
            
        final_score = min(score, 100)
        logger.info(f"Final insights score: {final_score}")
        return {
            'score': final_score,
            'details': insights.dict()
        }

    def _evaluate_output_formats(self, cleaned_data: bytes) -> Dict:
        """Validate output formats (Parquet/JSON)"""
        try:
            data = json.loads(cleaned_data)
            return {
                'score': 100,
                'format': 'json',
                'valid': True
            }
        except:
            return {
                'score': 0,
                'format': 'unknown',
                'valid': False
            }
