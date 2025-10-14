import os
import sys
import time
import json
import logging
import configparser
import requests            
import boto3
from botocore.config import Config
from typing import Dict, List, Any, Optional, Union, Callable
from config import *
from logger.logger_setup import logger as log
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class LLMExecutor:
    """Utility class for executing LLM calls with retries and error handling."""
    
    def __init__(self, provider="azure_openai", max_retries=3, retry_delay=2):
        """
        Initialize the LLM executor.
        
        Args:
            provider: The LLM provider to use ("azure_openai" or "aws_bedrock")
            max_retries: Maximum number of retries for failed API calls
            retry_delay: Initial delay between retries (will use exponential backoff)
        """
        self.provider = provider
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._load_config()
        
    def _load_config(self):
        """Load configuration for the selected provider."""
        # Read directly from config.ini file
        config_parser = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "config", "config.ini")
        
        if not os.path.exists(config_path):
            log.error(f"Config file not found at {config_path}")
            raise FileNotFoundError(f"Config file not found at {config_path}")
            
        config_parser.read(config_path)
        
        if self.provider == "azure_openai":
            if 'azure_openai' not in config_parser:
                log.error("azure_openai section not found in config.ini")
                raise ValueError("azure_openai section not found in config.ini")
                
            self.endpoint = config_parser.get('azure_openai', 'endpoint')
            self.model_name = config_parser.get('azure_openai', 'model_name')
            self.deployment = config_parser.get('azure_openai', 'deployment')
            self.api_key = config_parser.get('azure_openai', 'subscription_key')
            self.api_version = config_parser.get('azure_openai', 'api_version')
            
        elif self.provider == "aws_bedrock":
            if 'aws_bedrock' not in config_parser:
                log.error("aws_bedrock section not found in config.ini")
                raise ValueError("aws_bedrock section not found in config.ini")
                
            self.region = config_parser.get('aws_bedrock', 'region')
            self.model_id = config_parser.get('aws_bedrock', 'model_id')
            self.access_key_id = config_parser.get('aws_bedrock', 'access_key_id')
            self.secret_access_key = config_parser.get('aws_bedrock', 'secret_access_key')
            
            # Initialize AWS Bedrock client
            self.bedrock_client = self._init_bedrock_client()
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def _init_bedrock_client(self):
        """Initialize AWS Bedrock client."""
        try:
            boto_config = Config(
                region_name=self.region,
                signature_version="v4",
                retries={
                    'max_attempts': self.max_retries,
                    'mode': 'standard'
                }
            )
            
            return boto3.client(
                service_name='bedrock-runtime',
                config=boto_config,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key
            )
        except Exception as e:
            log.error(f"Failed to initialize AWS Bedrock client: {e}")
            raise
    
    def execute_with_retry(self, 
                          prompt: str, 
                          system_prompt: Optional[str] = None,
                          temperature: float = 0.7,
                          max_tokens: int = 1000,
                          stop_sequences: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Execute an LLM call with automatic retries on failure.
        
        Args:
            prompt: The user prompt to send to the LLM
            system_prompt: Optional system prompt for models that support it
            temperature: Controls randomness (0.0 to 1.0)
            max_tokens: Maximum tokens to generate in the response
            stop_sequences: Optional list of strings that will stop generation if encountered
            
        Returns:
            Dictionary containing the LLM response
        """
        retry_count = 0
        current_delay = self.retry_delay
        
        while retry_count <= self.max_retries:
            try:
                if self.provider == "azure_openai":
                    return self._execute_azure_openai(prompt, system_prompt, temperature, max_tokens, stop_sequences)
                elif self.provider == "aws_bedrock":
                    return self._execute_aws_bedrock(prompt, system_prompt, temperature, max_tokens, stop_sequences)
                else:
                    raise ValueError(f"Unsupported provider: {self.provider}")
                    
            except (requests.exceptions.RequestException, 
                    requests.exceptions.Timeout, 
                    ConnectionError) as e:
                # Network-related errors - retry
                retry_count += 1
                if retry_count <= self.max_retries:
                    log.warning(f"Network error on attempt {retry_count}, retrying in {current_delay}s: {e}")
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
                else:
                    log.error(f"Failed after {self.max_retries} attempts: {e}")
                    raise
                    
            except Exception as e:
                # Other errors - don't retry
                log.error(f"Error executing LLM call: {e}")
                raise
    
    def _execute_azure_openai(self, 
                             prompt: str, 
                             system_prompt: Optional[str], 
                             temperature: float,
                             max_tokens: int,
                             stop_sequences: Optional[List[str]]) -> Dict[str, Any]:
        """Execute a call to Azure OpenAI."""
        headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key
        }
        
        # Build the messages array
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }
        
        if stop_sequences:
            payload["stop"] = stop_sequences
            
        # Construct the API URL
        url = f"{self.endpoint}openai/deployments/{self.deployment}/chat/completions?api-version={self.api_version}"
        
        log.debug(f"Calling Azure OpenAI: {self.deployment}")
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        
        # Extract the actual response content
        if "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            return {
                "content": content,
                "raw_response": result
            }
        else:
            raise ValueError("Unexpected response format from Azure OpenAI")
    
    def _execute_aws_bedrock(self, 
                            prompt: str, 
                            system_prompt: Optional[str], 
                            temperature: float,
                            max_tokens: int,
                            stop_sequences: Optional[List[str]]) -> Dict[str, Any]:
        """Execute a call to AWS Bedrock."""
        # Check if we're using a Claude model
        is_claude = "claude" in self.model_id.lower()
        
        if is_claude:
            # Claude-specific payload
            payload = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature
            }
            
            # Build messages for Claude
            if system_prompt:
                payload["system"] = system_prompt
                
            payload["messages"] = [
                {"role": "user", "content": prompt}
            ]
            
            if stop_sequences:
                payload["stop_sequences"] = stop_sequences
                
        else:
            # Generic payload for other models
            payload = {
                "prompt": prompt,
                "max_tokens": max_tokens,
                "temperature": temperature
            }
            
            if system_prompt:
                payload["system_prompt"] = system_prompt
                
            if stop_sequences:
                payload["stop_sequences"] = stop_sequences
        
        # Convert payload to JSON string
        body = json.dumps(payload)
        
        log.debug(f"Calling AWS Bedrock: {self.model_id}")
        response = self.bedrock_client.invoke_model(
            modelId=self.model_id,
            body=body
        )
        
        # Parse the response body
        response_body = json.loads(response.get('body').read())
        
        # Extract content based on model type
        if is_claude:
            if "content" in response_body and len(response_body["content"]) > 0:
                content = response_body["content"][0]["text"]
                return {
                    "content": content,
                    "raw_response": response_body
                }
        else:
            # Generic extraction for other models
            if "completion" in response_body:
                return {
                    "content": response_body["completion"],
                    "raw_response": response_body
                }
                
        # If we get here, we couldn't extract the content
        raise ValueError(f"Unexpected response format from AWS Bedrock: {response_body}")
