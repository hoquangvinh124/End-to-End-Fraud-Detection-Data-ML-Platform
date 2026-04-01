from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import joblib
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from .models import (
    TransactionRequest, 
    PredictionResponse, 
    HealthResponse, 
    ErrorResponse
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global model variable
ml_model = None
model_version = None

# Feature columns order (must match training)
FEATURE_COLUMNS = [
    'TX_AMOUNT',
    'CUSTOMER_AVG_AMOUNT_WINDOW_1D',
    'CUSTOMER_AVG_AMOUNT_WINDOW_7D',
    'CUSTOMER_AVG_AMOUNT_WINDOW_30D',
    'CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D',
    'CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D',
    'CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D',
    'TERMINAL_RISK_1DAY_WINDOW',
    'TERMINAL_RISK_7DAY_WINDOW',
    'TERMINAL_RISK_30DAY_WINDOW',
    'TERMINAL_NB_TX_1DAY_WINDOW',
    'TERMINAL_NB_TX_7DAY_WINDOW',
    'TERMINAL_NB_TX_30DAY_WINDOW',
    'IS_WEEKEND',
    'IS_NIGHT'
]


def load_model():
    """Load the trained fraud detection model"""
    global ml_model, model_version
    
    try:
        # Find the latest model in ../models directory
        models_dir = Path(__file__).parent.parent / "models"
        
        if not models_dir.exists():
            raise FileNotFoundError(f"Models directory not found: {models_dir}")
        
        # Get all .pkl files
        model_files = sorted(models_dir.glob("fraud_detection_*.pkl"), reverse=True)
        
        if not model_files:
            raise FileNotFoundError(f"No model files found in {models_dir}")
        
        # Load the latest model
        model_path = model_files[0]
        logger.info(f"Loading model from: {model_path}")
        
        ml_model = joblib.load(model_path)
        model_version = model_path.stem  # Get filename without extension
        
        logger.info(f"Model loaded successfully: {model_version}")
        
    except Exception as e:
        logger.error(f"Error loading model: {str(e)}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup: Load model
    logger.info("Starting up API...")
    load_model()
    yield
    # Shutdown: Cleanup if needed
    logger.info("Shutting down API...")


# Initialize FastAPI app
app = FastAPI(
    title="Fraud Detection API",
    description="API for real-time credit card fraud detection using machine learning",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Fraud Detection API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy" if ml_model is not None else "unhealthy",
        model_loaded=ml_model is not None,
        model_version=model_version if model_version else "not_loaded"
    )


@app.post(
    "/predict",
    response_model=PredictionResponse,
    status_code=status.HTTP_200_OK,
    tags=["Prediction"],
    responses={
        200: {"description": "Successful prediction"},
        400: {"model": ErrorResponse, "description": "Invalid input data"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def predict_fraud(transaction: TransactionRequest):
    """
    Predict fraud probability for a transaction
    
    - **TX_AMOUNT**: Transaction amount (must be positive)
    - **CUSTOMER_AVG_AMOUNT_WINDOW_1D/7D/30D**: Customer's average transaction amounts
    - **CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D/7D/30D**: Number of customer transactions
    - **TERMINAL_RISK_1DAY/7DAY/30DAY_WINDOW**: Terminal fraud risk scores (0-1)
    - **TERMINAL_NB_TX_1DAY/7DAY/30DAY_WINDOW**: Number of terminal transactions
    - **IS_WEEKEND**: Whether transaction occurred on weekend
    - **IS_NIGHT**: Whether transaction occurred at night (10pm-6am)
    """
    try:
        if ml_model is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Model not loaded"
            )
        
        # Convert request to DataFrame with correct column order
        input_data = pd.DataFrame([transaction.model_dump()])[FEATURE_COLUMNS]
        
        # Get prediction probability
        fraud_probability = float(ml_model.predict_proba(input_data)[0, 1])
        
        # Get binary prediction (threshold = 0.5)
        is_fraud = bool(fraud_probability >= 0.5)
        
        logger.info(
            f"Prediction: fraud_prob={fraud_probability:.4f}, "
            f"is_fraud={is_fraud}"
        )
        
        return PredictionResponse(
            is_fraud=is_fraud,
            fraud_probability=round(fraud_probability, 4),
            model_version=model_version
        )
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}"
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            detail=str(exc)
        ).model_dump(exclude={'timestamp'})
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc)
        ).model_dump(exclude={'timestamp'})
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
