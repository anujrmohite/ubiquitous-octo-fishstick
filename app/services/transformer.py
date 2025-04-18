import json
import os
import yaml
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Union, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class RuleEngine:
    """
    Engine for applying transformation rules to DataFrame rows.
    
    Rules are defined in JSON or YAML format as key-value pairs where:
    - key: Output field name
    - value: Expression to evaluate
    
    Example:
    {
        "outfield1": "field1 + field2",
        "outfield2": "refdata1",
        "outfield3": "refdata2 + refdata3",
        "outfield4": "field3 * max(field5, refdata4)",
        "outfield5": "max(field5, refdata4)"
    }
    """
    
    def __init__(self, rules_file: Optional[str] = None, rules_dict: Optional[Dict[str, str]] = None):
        """
        Initialize the rule engine with rules from a file or dictionary.
        
        Args:
            rules_file: Path to rules file (JSON or YAML)
            rules_dict: Dictionary of rules
        """
        self.rules = {}
        
        if rules_file:
            self.load_rules_from_file(rules_file)
        
        if rules_dict:
            self.rules.update(rules_dict)
    
    def load_rules_from_file(self, rules_file: str) -> None:
        """
        Load rules from a JSON or YAML file.
        
        Args:
            rules_file: Path to rules file
        """
        if not os.path.exists(rules_file):
            raise FileNotFoundError(f"Rules file not found: {rules_file}")
        
        file_ext = Path(rules_file).suffix.lower()
        
        try:
            if file_ext == '.json':
                with open(rules_file, 'r') as f:
                    self.rules = json.load(f)
            elif file_ext in ['.yaml', '.yml']:
                with open(rules_file, 'r') as f:
                    self.rules = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported rules file format: {file_ext}")
                
            logger.info(f"Loaded rules from {rules_file}: {len(self.rules)} rules")
        except Exception as e:
            logger.error(f"Error loading rules from {rules_file}: {str(e)}")
            raise
    
    def save_rules_to_file(self, rules_file: str) -> None:
        """
        Save current rules to a JSON or YAML file.
        
        Args:
            rules_file: Path to save rules
        """
        file_ext = Path(rules_file).suffix.lower()
        
        try:
            os.makedirs(os.path.dirname(rules_file), exist_ok=True)
            
            if file_ext == '.json':
                with open(rules_file, 'w') as f:
                    json.dump(self.rules, f, indent=2)
            elif file_ext in ['.yaml', '.yml']:
                with open(rules_file, 'w') as f:
                    yaml.dump(self.rules, f)
            else:
                raise ValueError(f"Unsupported rules file format: {file_ext}")
                
            logger.info(f"Saved {len(self.rules)} rules to {rules_file}")
        except Exception as e:
            logger.error(f"Error saving rules to {rules_file}: {str(e)}")
            raise
    
    def update_rules(self, new_rules: Dict[str, str]) -> None:
        """
        Update the rules dictionary with new rules.
        
        Args:
            new_rules: Dictionary of new rules
        """
        self.rules.update(new_rules)
        logger.info(f"Updated rules: {len(self.rules)} total rules")
    
    def apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation rules to a DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame with output fields
        """
        if not self.rules:
            logger.warning("No rules defined, returning original DataFrame")
            return df
        
        result_df = pd.DataFrame(index=df.index)
        
        for output_field, expression in self.rules.items():
            try:
                # Create a safe local namespace for evaluating expressions
                safe_locals = {
                    "max": np.maximum,
                    "min": np.minimum,
                    "sum": np.sum,
                    "abs": np.abs,
                    "round": np.round
                }
                
                for col in df.columns:
                    safe_locals[col] = df[col]
                
                result = eval(expression, {"__builtins__": {}}, safe_locals)
                result_df[output_field] = result
                
                logger.debug(f"Applied rule for {output_field}: {expression}")
            except Exception as e:
                logger.error(f"Error applying rule for {output_field} ({expression}): {str(e)}")
                result_df[output_field] = np.nan
        
        return result_df
    
    def validate_rules(self, df: pd.DataFrame) -> Dict[str, bool]:
        """
        Validate that all rules can be applied to the given DataFrame.
        
        Args:
            df: DataFrame to validate against
            
        Returns:
            Dictionary of {rule: is_valid}
        """
        results = {}
        available_columns = set(df.columns)
        
        for output_field, expression in self.rules.items():
            try:
                tokens = [t.strip() for t in expression.replace('(', ' ').replace(')', ' ').replace('+', ' ').replace('-', ' ').replace('*', ' ').replace('/', ' ').split()]
                variables = [t for t in tokens if t not in ['max', 'min', 'sum', 'abs', 'round'] and not t.replace('.', '').isdigit()]
                
                missing_vars = [var for var in variables if var not in available_columns]
                
                if missing_vars:
                    results[output_field] = False
                    logger.warning(f"Rule for {output_field} references missing columns: {missing_vars}")
                else:
                    results[output_field] = True
                    
            except Exception as e:
                results[output_field] = False
                logger.error(f"Error validating rule for {output_field}: {str(e)}")
        
        return results