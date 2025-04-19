import json
import os
import yaml
import logging
import pandas as pd
import numpy as np
import math
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
            rules_file: Path to rules file (JSON or YAML). Exclusive with rules_dict.
            rules_dict: Dictionary of rules. Exclusive with rules_file.
        """
        self.rules = {}
        
        if rules_file and rules_dict:
            raise ValueError("Only one of 'rules_file' or 'rules_dict' should be provided.")

        if rules_file:
            self.load_rules_from_file(rules_file)
        elif rules_dict is not None:
            if not isinstance(rules_dict, dict):
                 raise TypeError("'rules_dict' must be a dictionary.")
            self.rules = rules_dict
        else:
            logger.warning("RuleEngine initialized without rules file or dictionary.")


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
            rules_data = {}
            if file_ext == '.json':
                with open(rules_file, 'r') as f:
                    rules_data = json.load(f)
            elif file_ext in ['.yaml', '.yml']:
                with open(rules_file, 'r') as f:
                    rules_data = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported rules file format: {file_ext}. Supported: .json, .yaml, .yml")

            if not isinstance(rules_data, dict):
                 raise TypeError(f"Rules file content is not a dictionary: {rules_file}")
                
            self.rules = rules_data
            logger.info(f"Loaded rules from {rules_file}: {len(self.rules)} rules")
        except (FileNotFoundError, ValueError, TypeError) as e:
            logger.error(f"Failed to load rules from {rules_file}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading rules from {rules_file}: {str(e)}")
            raise
    
    def save_rules_to_file(self, rules_file: str) -> None:
        """
        Save current rules to a JSON or YAML file.
        
        Args:
            rules_file: Path to save rules
        """
        file_ext = Path(rules_file).suffix.lower()
        
        if file_ext not in ['.json', '.yaml', '.yml']:
             raise ValueError(f"Unsupported file extension for saving rules: {file_ext}. Supported: .json, .yaml, .yml")

        try:
            os.makedirs(os.path.dirname(rules_file), exist_ok=True)
            
            if file_ext == '.json':
                with open(rules_file, 'w') as f:
                    json.dump(self.rules, f, indent=2, sort_keys=True, default=str)
            elif file_ext in ['.yaml', '.yml']:
                with open(rules_file, 'w') as f:
                    yaml.dump(self.rules, f, default_flow_style=False, sort_keys=True)
            
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
        if not isinstance(new_rules, dict):
             raise TypeError("'new_rules' must be a dictionary.")
        self.rules.update(new_rules)
        logger.info(f"Updated rules: {len(self.rules)} total rules")
    
    def apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation rules to a DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame with output fields. Returns an empty DataFrame if input is empty or no rules.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty, returning empty result.")
            if self.rules:
                 return pd.DataFrame(columns=list(df.columns) + list(self.rules.keys()))
            else:
                 return pd.DataFrame(columns=df.columns)

        if not self.rules:
            logger.warning("No rules defined, returning original DataFrame")
            return df.copy()

        required_cols = set()
        for expression in self.rules.values():
             tokens = [t.strip() for t in expression.replace('(', ' ').replace(')', ' ').replace('+', ' ').replace('-', ' ').replace('*', ' ').replace('/', ' ').replace(',', ' ').split()]
             variables = [t for t in tokens if t and not t.replace('.', '', 1).replace('-', '', 1).isdigit() 
                          and t not in ['max', 'min', 'sum', 'abs', 'round', 'if', 'else', 'and', 'or', 'not', 'is', 'None', 'True', 'False', 'in', 'for', 'while'] 
                          and not t.startswith(("\"", "'")) 
                         ]
             required_cols.update(variables)

        missing_input_cols = [col for col in required_cols if col not in df.columns]
        if missing_input_cols:
            logger.error(f"DataFrame missing required input columns for rules: {missing_input_cols}")

        result_df = df.copy()

        safe_globals = {}
        safe_locals_base = {
            "max": np.maximum,
            "min": np.minimum,
            "sum": np.sum,
            "abs": np.abs,
            "round": np.round,
            "np": np,
            "pd": pd,
            "isnan": np.isnan,
            "True": True, "False": False, "None": None,
            "int": int, "float": float, "str": str,
        }

        for output_field, expression in self.rules.items():
            try:
                safe_locals = safe_locals_base.copy()
                for col in result_df.columns:
                    safe_locals[col] = result_df[col]
                result = eval(expression, safe_globals, safe_locals)
                result_df[output_field] = result
                logger.debug(f"Applied rule for '{output_field}': '{expression}'")
            except (KeyError, NameError) as e:
                logger.warning(f"Rule for '{output_field}' failed (Column/Name not found: {e}). Setting output to NaN.")
                result_df[output_field] = np.nan
            except Exception as e:
                logger.error(f"Error applying rule for '{output_field}' ('{expression}'): {str(e)}. Setting output to NaN.")
                result_df[output_field] = np.nan
        
        return result_df
    
    def validate_rules(self, df: pd.DataFrame) -> Dict[str, bool]:
        """
        Validate that all rules can be applied to the given DataFrame.
        Checks if columns/variables referenced in rules exist in the DataFrame.
        Note: This is a static check based on column names. It doesn't guarantee
        that the expression will evaluate successfully for all data types or values.
        
        Args:
            df: DataFrame to validate against
            
        Returns:
            Dictionary of {rule_name: is_valid}
        """
        results = {}
        available_columns = set(df.columns)

        safe_names_base = {
            "max", "min", "sum", "abs", "round",
            "np", "pd", "isnan",
            "True", "False", "None", "int", "float", "str",
        }
        
        for output_field, expression in self.rules.items():
            try:
                tokens = [t.strip() for t in expression.replace('(', ' ').replace(')', ' ').replace('+', ' ').replace('-', ' ').replace('*', ' ').replace('/', ' ').replace(',', ' ').split()]
                variables = [t for t in tokens if t and not t.replace('.', '', 1).replace('-', '', 1).isdigit()
                             and t not in safe_names_base
                             and t not in ['if', 'else', 'and', 'or', 'not', 'is', 'in', 'for', 'while']
                             and not t.startswith(("\"", "'"))
                            ]
                missing_vars = [var for var in variables if var not in available_columns]
                
                if missing_vars:
                    results[output_field] = False
                else:
                    results[output_field] = True
                    
            except Exception as e:
                results[output_field] = False
                logger.error(f"Error parsing or validating rule expression for '{output_field}': {str(e)}")
        
        return results