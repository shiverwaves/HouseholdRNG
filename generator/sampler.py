"""
Weighted sampling utilities for household generation.

All sampling uses weighted probabilities from PUMS/BLS distributions.
"""

import numpy as np
import pandas as pd
from typing import Union


def weighted_sample(df: pd.DataFrame, weight_col: str = 'weighted_count') -> pd.Series:
    """
    Sample one row from a DataFrame using weighted probabilities.
    
    Args:
        df: DataFrame with distribution data
        weight_col: Column containing weights (population counts)
    
    Returns:
        Single row as Series
    
    Example:
        >>> patterns = pd.DataFrame({
        ...     'pattern': ['A', 'B', 'C'],
        ...     'weighted_count': [100, 200, 700]
        ... })
        >>> row = weighted_sample(patterns)
        >>> row['pattern']  # More likely to be 'C' (70% probability)
    """
    if len(df) == 0:
        raise ValueError("Cannot sample from empty DataFrame")
    
    if weight_col not in df.columns:
        raise ValueError(f"Weight column '{weight_col}' not found in DataFrame")
    
    weights = df[weight_col].values
    
    # Handle edge cases
    if weights.sum() == 0:
        raise ValueError("All weights are zero - cannot sample")
    
    # Calculate probabilities
    probs = weights / weights.sum()
    
    # Sample
    idx = np.random.choice(len(df), p=probs)
    return df.iloc[idx]


def sample_from_bracket(bracket_str: str) -> int:
    """
    Sample a value from a bracket string like "$25-50K".
    
    Args:
        bracket_str: Bracket string (e.g., "<$25K", "$25-50K", "$200K+")
    
    Returns:
        Random value within the bracket range
    
    Examples:
        >>> sample_from_bracket("<$25K")
        12450  # Random value between 0 and 25,000
        
        >>> sample_from_bracket("$25-50K")
        37800  # Random value between 25,000 and 50,000
        
        >>> sample_from_bracket("$200K+")
        245000  # Random value >= 200,000 (exponential distribution)
    """
    bracket_str = bracket_str.strip()
    
    # Less than (e.g., "<$25K")
    if bracket_str.startswith('<'):
        max_val = parse_dollar_amount(bracket_str[1:])
        return np.random.randint(0, max_val + 1)
    
    # Greater than (e.g., "$200K+")
    elif bracket_str.endswith('+'):
        min_val = parse_dollar_amount(bracket_str[:-1])
        # Use exponential distribution with mean of 50K above minimum
        return min_val + int(np.random.exponential(50000))
    
    # Range (e.g., "$25-50K")
    elif '-' in bracket_str:
        parts = bracket_str.split('-')
        min_val = parse_dollar_amount(parts[0])
        max_val = parse_dollar_amount(parts[1])
        return np.random.randint(min_val, max_val + 1)
    
    # Single value (e.g., "$25K")
    else:
        return parse_dollar_amount(bracket_str)


def parse_dollar_amount(s: str) -> int:
    """
    Parse dollar string like "$25K" to integer.
    
    Args:
        s: Dollar string
    
    Returns:
        Integer dollar amount
    
    Examples:
        >>> parse_dollar_amount("$25K")
        25000
        >>> parse_dollar_amount("$1.5M")
        1500000
        >>> parse_dollar_amount("$5000")
        5000
    """
    s = s.replace('$', '').replace(',', '').strip()
    
    if s.endswith('K'):
        return int(float(s[:-1]) * 1000)
    elif s.endswith('M'):
        return int(float(s[:-1]) * 1000000)
    else:
        return int(float(s))


def get_age_bracket(age: int, distribution_df: pd.DataFrame) -> str:
    """
    Find which age bracket an age falls into.
    
    Args:
        age: Person's age
        distribution_df: DataFrame with 'age_bracket' column
    
    Returns:
        Age bracket string (e.g., '25-29')
    
    Example:
        >>> df = pd.DataFrame({'age_bracket': ['18-24', '25-29', '30-34']})
        >>> get_age_bracket(27, df)
        '25-29'
    """
    for bracket in distribution_df['age_bracket'].unique():
        if match_age_bracket(age, bracket):
            return bracket
    
    # Fallback: return closest bracket
    return distribution_df['age_bracket'].iloc[0]


def match_age_bracket(age: int, bracket: str) -> bool:
    """
    Check if an age falls within a bracket string.
    
    Args:
        age: Person's age
        bracket: Bracket string (e.g., '25-29', '<18', '75+')
    
    Returns:
        True if age is in bracket
    """
    bracket = bracket.strip()
    
    # Less than (e.g., '<18')
    if bracket.startswith('<'):
        max_age = int(bracket[1:])
        return age < max_age
    
    # Greater than or equal (e.g., '75+')
    elif bracket.endswith('+'):
        min_age = int(bracket[:-1])
        return age >= min_age
    
    # Range (e.g., '25-29')
    elif '-' in bracket:
        parts = bracket.split('-')
        min_age = int(parts[0])
        max_age = int(parts[1])
        return min_age <= age <= max_age
    
    return False
