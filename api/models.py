from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class TransactionRequest(BaseModel):
    """Request model cho fraud detection prediction"""
    
    TX_AMOUNT: float = Field(..., description="Transaction amount", gt=0)
    CUSTOMER_AVG_AMOUNT_WINDOW_1D: float = Field(default=0.0, ge=0, description="Customer avg amount in 1 day window")
    CUSTOMER_AVG_AMOUNT_WINDOW_7D: float = Field(default=0.0, ge=0, description="Customer avg amount in 7 day window")
    CUSTOMER_AVG_AMOUNT_WINDOW_30D: float = Field(default=0.0, ge=0, description="Customer avg amount in 30 day window")
    CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D: float = Field(default=0.0, ge=0, description="Number of transactions in 1 day window")
    CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D: float = Field(default=0.0, ge=0, description="Number of transactions in 7 day window")
    CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D: float = Field(default=0.0, ge=0, description="Number of transactions in 30 day window")
    TERMINAL_RISK_1DAY_WINDOW: float = Field(default=0.0, ge=0, le=1, description="Terminal fraud risk in 1 day window")
    TERMINAL_RISK_7DAY_WINDOW: float = Field(default=0.0, ge=0, le=1, description="Terminal fraud risk in 7 day window")
    TERMINAL_RISK_30DAY_WINDOW: float = Field(default=0.0, ge=0, le=1, description="Terminal fraud risk in 30 day window")
    TERMINAL_NB_TX_1DAY_WINDOW: float = Field(default=0.0, ge=0, description="Number of transactions at terminal in 1 day window")
    TERMINAL_NB_TX_7DAY_WINDOW: float = Field(default=0.0, ge=0, description="Number of transactions at terminal in 7 day window")
    TERMINAL_NB_TX_30DAY_WINDOW: float = Field(default=0.0, ge=0, description="Number of transactions at terminal in 30 day window")
    IS_WEEKEND: bool = Field(..., description="Whether transaction is on weekend")
    IS_NIGHT: bool = Field(..., description="Whether transaction is during night time (10pm-6am)")

    class Config:
        json_schema_extra = {
            "example": {
                "TX_AMOUNT": 150.50,
                "CUSTOMER_AVG_AMOUNT_WINDOW_1D": 120.0,
                "CUSTOMER_AVG_AMOUNT_WINDOW_7D": 110.5,
                "CUSTOMER_AVG_AMOUNT_WINDOW_30D": 105.3,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D": 2.0,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D": 8.0,
                "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D": 25.0,
                "TERMINAL_RISK_1DAY_WINDOW": 0.02,
                "TERMINAL_RISK_7DAY_WINDOW": 0.015,
                "TERMINAL_RISK_30DAY_WINDOW": 0.01,
                "TERMINAL_NB_TX_1DAY_WINDOW": 50.0,
                "TERMINAL_NB_TX_7DAY_WINDOW": 300.0,
                "TERMINAL_NB_TX_30DAY_WINDOW": 1200.0,
                "IS_WEEKEND": False,
                "IS_NIGHT": False
            }
        }


class PredictionResponse(BaseModel):
    """Response model cho fraud detection prediction"""
    
    is_fraud: bool = Field(..., description="Predicted fraud label (True/False)")
    fraud_probability: float = Field(..., ge=0, le=1, description="Fraud probability score (0-1)")
    timestamp: datetime = Field(default_factory=datetime.now, description="Prediction timestamp")
    model_version: str = Field(..., description="Model version used for prediction")

    class Config:
        json_schema_extra = {
            "example": {
                "is_fraud": False,
                "fraud_probability": 0.05,
                "timestamp": "2026-01-17T10:30:00",
                "model_version": "xgboost_20260112_130642"
            }
        }


class HealthResponse(BaseModel):
    """Health check response"""
    
    status: str = Field(..., description="API status")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    model_version: str = Field(..., description="Loaded model version")
    timestamp: datetime = Field(default_factory=datetime.now, description="Check timestamp")


class ErrorResponse(BaseModel):
    """Error response model"""
    
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(default_factory=datetime.now, description="Error timestamp")
