"""
Weighted sampling utilities for household generation.

All sampling uses weighted probabilities from PUMS/BLS distributions.
"""

import numpy as np
import pandas as pd
from typing import Union


def weighted_sample(
    df: pd.DataFrame, 
    weight_col: str = 'weighted_count',
    n: int = 1
) -> Union[pd.Series, pd.DataFrame]:
    """
    Sample rows from a DataFrame using weighted probabilities.
    
    Args:
        df: DataFrame with distribution data
        weight_col: Column containing weights (population counts)
        n: Number of samples to draw
    
    Returns:
        Single row as Series (if n=1) or DataFrame (if n>1)
    """
    if len(df) == 0:
        raise ValueError("Cannot sample from empty DataFrame")
    
    if weight_col not in df.columns:
        raise ValueError(f"Weight column '{weight_col}' not found")
    
    weights = df[weight_col].values.astype(float)
    
    if weights.sum() == 0:
        raise ValueError("All weights are zero - cannot sample")
    
    # Normalize to probabilities
    probs = weights / weights.sum()
    
    # Sample indices
    indices = np.random.choice(len(df), size=n, p=probs, replace=True)
    
    if n == 1:
        return df.iloc[indices[0]]
    return df.iloc[indices]


def sample_from_bracket(bracket_str: str) -> int:
    """
    Sample a value from a bracket string like "$25-50K".
    
    Args:
        bracket_str: Bracket string (e.g., "<$25K", "$25-50K", "$200K+")
    
    Returns:
        Random value within the bracket range
    """
    bracket_str = str(bracket_str).strip()
    
    # Less than (e.g., "<$25K")
    if bracket_str.startswith('<'):
        max_val = parse_dollar_amount(bracket_str[1:])
        return np.random.randint(0, max(1, max_val))
    
    # Greater than (e.g., "$200K+")
    if bracket_str.endswith('+'):
        min_val = parse_dollar_amount(bracket_str[:-1])
        # Exponential distribution with mean 50K above minimum
        return min_val + int(np.random.exponential(50000))
    
    # Range (e.g., "$25-50K")
    if '-' in bracket_str:
        parts = bracket_str.split('-')
        min_val = parse_dollar_amount(parts[0])
        max_val = parse_dollar_amount(parts[1])
        return np.random.randint(min_val, max(min_val + 1, max_val))
    
    # Single value
    return parse_dollar_amount(bracket_str)


def parse_dollar_amount(s: str) -> int:
    """
    Parse dollar string like "$25K" to integer.
    
    Args:
        s: Dollar string
    
    Returns:
        Integer dollar amount
    """
    s = str(s).replace('$', '').replace(',', '').strip()
    
    if not s:
        return 0
    
    if s.upper().endswith('K'):
        return int(float(s[:-1]) * 1000)
    if s.upper().endswith('M'):
        return int(float(s[:-1]) * 1000000)
    
    try:
        return int(float(s))
    except ValueError:
        return 0


def get_age_bracket(age: int, brackets: list) -> str:
    """
    Find which age bracket an age falls into.
    
    Args:
        age: Person's age
        brackets: List of bracket strings (e.g., ['18-24', '25-34', '35-44'])
    
    Returns:
        Matching bracket string
    """
    for bracket in brackets:
        if match_age_bracket(age, bracket):
            return bracket
    
    # Return closest bracket if no match
    return brackets[-1] if brackets else '18-24'


def match_age_bracket(age: int, bracket: str) -> bool:
    """
    Check if an age falls within a bracket string.
    
    Args:
        age: Person's age
        bracket: Bracket string (e.g., '25-29', '<18', '75+')
    
    Returns:
        True if age is in bracket
    """
    bracket = str(bracket).strip()
    
    # Less than (e.g., '<18')
    if bracket.startswith('<'):
        try:
            max_age = int(bracket[1:])
            return age < max_age
        except ValueError:
            return False
    
    # Greater than or equal (e.g., '75+')
    if bracket.endswith('+'):
        try:
            min_age = int(bracket[:-1])
            return age >= min_age
        except ValueError:
            return False
    
    # Range (e.g., '25-29')
    if '-' in bracket:
        try:
            parts = bracket.split('-')
            min_age = int(parts[0])
            max_age = int(parts[1])
            return min_age <= age <= max_age
        except (ValueError, IndexError):
            return False
    
    # Single value
    try:
        return age == int(bracket)
    except ValueError:
        return False


def sample_age_from_bracket(bracket: str) -> int:
    """
    Sample an age from an age bracket.
    
    Args:
        bracket: Age bracket string (e.g., '25-34', '65+', '<18')
    
    Returns:
        Random age within bracket
    """
    bracket = str(bracket).strip()
    
    # Less than
    if bracket.startswith('<'):
        max_age = int(bracket[1:])
        return np.random.randint(0, max_age)
    
    # Greater than or equal
    if bracket.endswith('+'):
        min_age = int(bracket[:-1])
        # Sample with decreasing probability as age increases
        return min_age + int(np.random.exponential(10))
    
    # Range
    if '-' in bracket:
        parts = bracket.split('-')
        min_age = int(parts[0])
        max_age = int(parts[1])
        return np.random.randint(min_age, max_age + 1)
    
    # Single value
    return int(bracket)


def set_random_seed(seed: int) -> None:
    """
    Set random seed for reproducibility.
    
    Args:
        seed: Random seed value
    """
    np.random.seed(seed)
