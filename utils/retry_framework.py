#!/usr/bin/env python3
"""
Industry-Grade Retry and Fallback Framework

Clean, modular, testable retry mechanism following SOLID principles.
Separation of concerns: Strategy Pattern + Factory Pattern + Chain of Responsibility
"""

import time
import logging
from abc import ABC, abstractmethod
from typing import Callable, Any, Optional, Type, Union
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger("retry_framework")


class OperationType(Enum):
    """Supported operation types for retry strategies"""
    GMAIL_API = "gmail_api"
    MONGODB = "mongodb" 
    FILE_OPERATION = "file_operation"


@dataclass
class RetryResult:
    """Result container for retry operations"""
    success: bool
    result: Any = None
    attempts_made: int = 0
    total_delay: float = 0.0
    final_exception: Optional[Exception] = None


class RetryStrategy(ABC):
    """Abstract base class for retry strategies"""
    
    @abstractmethod
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number (0-based)"""
        pass
    
    @abstractmethod
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if we should retry based on exception and attempt"""
        pass


class FixedDelayStrategy(RetryStrategy):
    """Fixed delay retry strategy"""
    
    def __init__(self, delay_seconds: float, max_attempts: int = 3):
        self.delay_seconds = delay_seconds
        self.max_attempts = max_attempts
    
    def calculate_delay(self, attempt: int) -> float:
        return self.delay_seconds
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        return attempt < self.max_attempts - 1


class ExponentialBackoffStrategy(RetryStrategy):
    """Exponential backoff retry strategy"""
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 30.0, 
                 multiplier: float = 2.0, max_attempts: int = 3):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.max_attempts = max_attempts
    
    def calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay * (self.multiplier ** attempt)
        return min(delay, self.max_delay)
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        return attempt < self.max_attempts - 1


class FallbackAction(Enum):
    """Available fallback actions when all retries fail"""
    RAISE_EXCEPTION = "raise"
    RETURN_NONE = "return_none"
    RETURN_DEFAULT = "return_default"
    LOG_AND_CONTINUE = "log_continue"


class FallbackHandler:
    """Handles fallback behavior when retries are exhausted"""
    
    def __init__(self, action: FallbackAction, default_value: Any = None):
        self.action = action
        self.default_value = default_value
    
    def handle(self, exception: Exception, operation_name: str, attempts: int) -> Any:
        """Execute fallback action"""
        if self.action == FallbackAction.RAISE_EXCEPTION:
            logger.error(f"{operation_name} failed after {attempts} attempts: {exception}")
            raise exception
            
        elif self.action == FallbackAction.RETURN_NONE:
            logger.error(f"{operation_name} failed after {attempts} attempts: {exception}")
            logger.warning(f"Returning None for {operation_name}")
            return None
            
        elif self.action == FallbackAction.RETURN_DEFAULT:
            logger.error(f"{operation_name} failed after {attempts} attempts: {exception}")
            logger.warning(f"Returning default value for {operation_name}: {self.default_value}")
            return self.default_value
            
        elif self.action == FallbackAction.LOG_AND_CONTINUE:
            logger.error(f"{operation_name} failed after {attempts} attempts: {exception}")
            logger.info(f"Continuing execution despite {operation_name} failure")
            return None
            
        else:
            raise ValueError(f"Unknown fallback action: {self.action}")


class RetryExecutor:
    """Main executor that orchestrates retry logic with strategy and fallback"""
    
    def __init__(self, strategy: RetryStrategy, fallback: FallbackHandler):
        self.strategy = strategy
        self.fallback = fallback
    
    def execute(self, func: Callable, *args, operation_name: str = "operation", **kwargs) -> RetryResult:
        """Execute function with retry logic and fallback handling"""
        attempt = 0
        total_delay = 0.0
        last_exception = None
        
        while True:
            try:
                logger.debug(f"Executing {operation_name} (attempt {attempt + 1})")
                result = func(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"{operation_name} succeeded on attempt {attempt + 1} after {total_delay:.1f}s")
                
                return RetryResult(
                    success=True, 
                    result=result, 
                    attempts_made=attempt + 1,
                    total_delay=total_delay
                )
                
            except Exception as e:
                last_exception = e
                logger.warning(f"{operation_name} failed (attempt {attempt + 1}): {e}")
                
                if not self.strategy.should_retry(e, attempt):
                    logger.error(f"{operation_name} exhausted all retries after {attempt + 1} attempts")
                    break
                
                delay = self.strategy.calculate_delay(attempt)
                total_delay += delay
                logger.info(f"Retrying {operation_name} in {delay:.1f}s (total delay: {total_delay:.1f}s)")
                time.sleep(delay)
                attempt += 1
        
        # All retries failed - invoke fallback
        fallback_result = self.fallback.handle(last_exception, operation_name, attempt + 1)
        
        return RetryResult(
            success=False,
            result=fallback_result,
            attempts_made=attempt + 1,
            total_delay=total_delay,
            final_exception=last_exception
        )


class RetryFactory:
    """Factory for creating pre-configured retry executors for different operations"""
    
    @staticmethod
    def gmail_executor() -> RetryExecutor:
        """
        Gmail API retry executor:
        - 65 second fixed delay (quota management)
        - 3 attempts maximum  
        - Returns None on failure (preserves previous JSON data)
        """
        strategy = FixedDelayStrategy(delay_seconds=65, max_attempts=3)
        fallback = FallbackHandler(
            action=FallbackAction.LOG_AND_CONTINUE
        )
        return RetryExecutor(strategy, fallback)
    
    @staticmethod
    def mongodb_executor() -> RetryExecutor:
        """
        MongoDB retry executor:
        - Exponential backoff (1s → 2s → 4s)
        - 30 second maximum delay
        - 3 attempts maximum
        - Returns None on failure (graceful degradation)
        """
        strategy = ExponentialBackoffStrategy(
            base_delay=1.0, 
            max_delay=30.0, 
            multiplier=2.0, 
            max_attempts=3
        )
        fallback = FallbackHandler(action=FallbackAction.RETURN_NONE)
        return RetryExecutor(strategy, fallback)
    
    @staticmethod
    def file_executor() -> RetryExecutor:
        """
        File operation retry executor:
        - 1 second fixed delay
        - 3 attempts maximum
        - Returns empty dict on failure
        """
        strategy = FixedDelayStrategy(delay_seconds=1.0, max_attempts=3)
        fallback = FallbackHandler(
            action=FallbackAction.RETURN_DEFAULT,
            default_value={}
        )
        return RetryExecutor(strategy, fallback)


# Convenience wrapper for backward compatibility
class RetryService:
    """High-level service interface for retry operations"""
    
    def __init__(self):
        self._executors = {
            OperationType.GMAIL_API: RetryFactory.gmail_executor(),
            OperationType.MONGODB: RetryFactory.mongodb_executor(),
            OperationType.FILE_OPERATION: RetryFactory.file_executor()
        }
    
    def execute(self, operation_type: OperationType, func: Callable, 
                *args, operation_name: str = None, **kwargs) -> Any:
        """Execute function with appropriate retry strategy"""
        if operation_name is None:
            operation_name = f"{operation_type.value}_operation"
            
        executor = self._executors[operation_type]
        result = executor.execute(func, *args, operation_name=operation_name, **kwargs)
        return result.result
    
    def gmail_retry(self, func: Callable, *args, operation_name: str = "Gmail API", **kwargs) -> Any:
        """Gmail API retry with 65s fixed delay"""
        return self.execute(OperationType.GMAIL_API, func, *args, operation_name=operation_name, **kwargs)
    
    def mongodb_retry(self, func: Callable, *args, operation_name: str = "MongoDB", **kwargs) -> Any:
        """MongoDB retry with exponential backoff"""
        return self.execute(OperationType.MONGODB, func, *args, operation_name=operation_name, **kwargs)
    
    def file_retry(self, func: Callable, *args, operation_name: str = "File operation", **kwargs) -> Any:
        """File operation retry with 1s fixed delay"""
        return self.execute(OperationType.FILE_OPERATION, func, *args, operation_name=operation_name, **kwargs)


# Global service instance for easy access
retry_service = RetryService()
