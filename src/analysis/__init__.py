"""
Options Analysis Module
======================
Clean MVC architecture for options trading analysis.

This module provides a complete options analysis pipeline with:
- Data models and entities
- Business logic services
- Controllers for workflow orchestration
- Views for data export
- Configuration management
"""

from .main import OptionsAnalysisApp
from .controllers.analysis_controller import AnalysisController
from .controllers.data_processor_controller import DataProcessorController
from .models.entities import (
    Stock, StockPrediction, PredictionResult, 
    AnalysisResultDTO, ExportResultDTO
)
from .config.settings import (
    trading_config, analysis_config, processing_config,
    database_config, api_config
)

__version__ = "2.0.0"
__author__ = "Options Analysis Team"

# Main application class for easy import
__all__ = [
    'OptionsAnalysisApp',
    'AnalysisController', 
    'DataProcessorController',
    'Stock',
    'StockPrediction',
    'PredictionResult',
    'AnalysisResultDTO',
    'ExportResultDTO',
    'trading_config',
    'analysis_config',
    'processing_config',
    'database_config',
    'api_config'
] 