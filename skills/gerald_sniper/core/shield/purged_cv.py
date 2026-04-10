# core/shield/purged_cv.py
import pandas as pd
import numpy as np
from typing import Generator, Tuple, List

class PurgedKFold:
    """
    [GEKTOR v21.6] Institutional Cross-Validation (Lopes de Prado).
    - Purging: Eliminates training observations where labels overlap with the test set.
    - Embargoing: Adds a buffer period after the test set to handle serial correlation in features.
    
    This is the ONLY way to prevent Data Leakage in medium-term strategies.
    """
    def __init__(self, n_splits: int = 5, pct_embargo: float = 0.01):
        self.n_splits = n_splits
        self.pct_embargo = pct_embargo

    def split(self, X: pd.DataFrame, y: pd.Series, t1: pd.Series) -> Generator[Tuple[np.ndarray, np.ndarray], None, None]:
        """
        X: Features
        y: Binary Labels (Meta-Labels)
        t1: Series with index as signal time and values as exit time (Triple Barrier end)
        """
        if not isinstance(X.index, pd.DatetimeIndex):
            raise ValueError("X must have a DatetimeIndex for purging.")
            
        indices = np.arange(X.shape[0])
        # Divide indices into n_splits (Time-Series Split)
        test_starts = [(i * X.shape[0] // self.n_splits) for i in range(self.n_splits)]
        
        for i in range(self.n_splits):
            test_indices = indices[test_starts[i] : (test_starts[i] + X.shape[0] // self.n_splits)]
            
            t_start = X.index[test_indices[0]]
            t_end = t1.iloc[test_indices[-1]] # The actual horizon end of the last test observation
            
            # [PURGING] Find training samples that don't overlap with test horizon
            train_indices = self._get_purged_train_indices(X, t1, test_indices, t_start, t_end)
            
            yield train_indices, test_indices

    def _get_purged_train_indices(self, X: pd.DataFrame, t1: pd.Series, test_indices: np.ndarray, t_start: pd.Timestamp, t_end: pd.Timestamp) -> np.ndarray:
        """
        Removes samples from train set that overlap with the test window [t_start, t_end].
        Also applies Embargo after t_end.
        """
        train_indices = np.arange(X.shape[0])
        
        # 1. Purge observations that overlap with the test set's horizon
        # A train observation (start_i, end_i) overlaps if:
        # (start_i <= t_end) AND (end_i >= t_start)
        
        # We define masks
        is_overlapping = (t1.index <= t_end) & (t1 >= t_start)
        
        # 2. [EMBARGO] Remove samples for a fixed period after t_end
        # To prevent serial correlation leakage (e.g. from VPIN features)
        embargo_period = int(X.shape[0] * self.pct_embargo)
        last_test_idx = test_indices[-1]
        embargo_indices = np.arange(last_test_idx, min(last_test_idx + embargo_period, X.shape[0]))
        
        # Final train set is all indices NOT in test, NOT overlapping, and NOT in embargo
        mask = ~is_overlapping.values
        train_indices = train_indices[mask]
        
        # Remove direct test indices and embargo
        invalid_set = set(test_indices).union(set(embargo_indices))
        train_indices = np.array([idx for idx in train_indices if idx not in invalid_set])
        
        return train_indices
