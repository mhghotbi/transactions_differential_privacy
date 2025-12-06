"""
Histogram Structure for Transaction Data.

Defines the multi-dimensional histogram structure for storing
aggregated transaction data with support for DP operations.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class HistogramDimension:
    """Represents a dimension of the histogram."""
    name: str
    size: int
    labels: Optional[List[str]] = None
    
    def get_label(self, index: int) -> str:
        """Get label for an index."""
        if self.labels and 0 <= index < len(self.labels):
            return self.labels[index]
        return str(index)


@dataclass
class TransactionHistogram:
    """
    Multi-dimensional histogram for transaction data.
    
    Dimensions:
    - province: Province index
    - city: City index (within province context)
    - mcc: MCC code index
    - day: Day index (0-29 for 30 days)
    
    Each cell contains 3 values (one per query):
    - transaction_count: Number of transactions
    - unique_cards: Number of unique card numbers
    - total_amount: Sum of transaction amounts (winsorized)
    """
    
    QUERIES = ['transaction_count', 'unique_cards', 'total_amount']
    
    def __init__(
        self,
        province_dim: int,
        city_dim: int,
        mcc_dim: int,
        day_dim: int = 30,
        province_labels: Optional[List[str]] = None,
        city_labels: Optional[List[str]] = None,
        mcc_labels: Optional[List[str]] = None
    ):
        """
        Initialize histogram structure.
        
        Args:
            province_dim: Number of provinces
            city_dim: Maximum number of cities per province
            mcc_dim: Number of unique MCC codes
            day_dim: Number of days (default 30)
            province_labels: Optional province names
            city_labels: Optional city names
            mcc_labels: Optional MCC codes
        """
        self.dimensions = {
            'province': HistogramDimension('province', province_dim, province_labels),
            'city': HistogramDimension('city', city_dim, city_labels),
            'mcc': HistogramDimension('mcc', mcc_dim, mcc_labels),
            'day': HistogramDimension('day', day_dim, [str(i) for i in range(day_dim)])
        }
        
        self.shape = (province_dim, city_dim, mcc_dim, day_dim)
        
        # Initialize data arrays for each query
        self.data: Dict[str, np.ndarray] = {
            query: np.zeros(self.shape, dtype=np.int64)
            for query in self.QUERIES
        }
        
        # Track which cells have data
        self._has_data = np.zeros(self.shape, dtype=bool)
    
    @property
    def total_cells(self) -> int:
        """Total number of cells in the histogram."""
        return int(np.prod(self.shape))
    
    @property
    def non_zero_cells(self) -> int:
        """Number of cells with data."""
        return int(np.sum(self._has_data))
    
    def set_value(
        self,
        province_idx: int,
        city_idx: int,
        mcc_idx: int,
        day_idx: int,
        query: str,
        value: int
    ) -> None:
        """Set a value in the histogram."""
        if query not in self.QUERIES:
            raise ValueError(f"Unknown query: {query}")
        
        self.data[query][province_idx, city_idx, mcc_idx, day_idx] = value
        self._has_data[province_idx, city_idx, mcc_idx, day_idx] = True
    
    def get_value(
        self,
        province_idx: int,
        city_idx: int,
        mcc_idx: int,
        day_idx: int,
        query: str
    ) -> int:
        """Get a value from the histogram."""
        if query not in self.QUERIES:
            raise ValueError(f"Unknown query: {query}")
        
        return int(self.data[query][province_idx, city_idx, mcc_idx, day_idx])
    
    def get_all_values(
        self,
        province_idx: int,
        city_idx: int,
        mcc_idx: int,
        day_idx: int
    ) -> Dict[str, int]:
        """Get all query values for a cell."""
        return {
            query: int(self.data[query][province_idx, city_idx, mcc_idx, day_idx])
            for query in self.QUERIES
        }
    
    def aggregate_to_province(self, query: str) -> np.ndarray:
        """
        Aggregate a query to province level (sum over cities, mccs, days).
        
        Args:
            query: Query name
            
        Returns:
            1D array of shape (province_dim,)
        """
        return np.sum(self.data[query], axis=(1, 2, 3))
    
    def aggregate_to_city(self, query: str) -> np.ndarray:
        """
        Aggregate a query to city level (sum over mccs, days).
        
        Args:
            query: Query name
            
        Returns:
            2D array of shape (province_dim, city_dim)
        """
        return np.sum(self.data[query], axis=(2, 3))
    
    def aggregate_to_day(self, query: str) -> np.ndarray:
        """
        Aggregate a query to day level (sum over geography).
        
        Args:
            query: Query name
            
        Returns:
            1D array of shape (day_dim,)
        """
        return np.sum(self.data[query], axis=(0, 1, 2))
    
    def get_query_array(self, query: str) -> np.ndarray:
        """Get the full array for a query."""
        return self.data[query]
    
    def set_query_array(self, query: str, array: np.ndarray) -> None:
        """Set the full array for a query."""
        if array.shape != self.shape:
            raise ValueError(f"Array shape {array.shape} doesn't match histogram shape {self.shape}")
        self.data[query] = array.astype(np.int64)
    
    def copy(self) -> 'TransactionHistogram':
        """Create a deep copy of the histogram."""
        new_hist = TransactionHistogram(
            province_dim=self.shape[0],
            city_dim=self.shape[1],
            mcc_dim=self.shape[2],
            day_dim=self.shape[3],
            province_labels=self.dimensions['province'].labels,
            city_labels=self.dimensions['city'].labels,
            mcc_labels=self.dimensions['mcc'].labels
        )
        
        for query in self.QUERIES:
            new_hist.data[query] = self.data[query].copy()
        new_hist._has_data = self._has_data.copy()
        
        return new_hist
    
    def to_records(self) -> List[Dict[str, Any]]:
        """
        Convert histogram to list of records (for DataFrame conversion).
        
        Uses NumPy vectorization for fast conversion.
        
        Returns:
            List of dicts with province, city, mcc, day, and query values
        """
        # Use NumPy to find non-zero cells efficiently
        indices = np.where(self._has_data)
        
        if len(indices[0]) == 0:
            return []
        
        # Pre-fetch all labels
        province_labels = self.dimensions['province'].labels or [str(i) for i in range(self.shape[0])]
        city_labels = self.dimensions['city'].labels or [str(i) for i in range(self.shape[1])]
        mcc_labels = self.dimensions['mcc'].labels or [str(i) for i in range(self.shape[2])]
        day_labels = self.dimensions['day'].labels or [str(i) for i in range(self.shape[3])]
        
        # Pre-fetch query data for all non-zero cells
        query_data = {
            query: self.data[query][indices].astype(int)
            for query in self.QUERIES
        }
        
        # Build records using vectorized operations
        records = []
        num_records = len(indices[0])
        
        for i in range(num_records):
            p, c, m, d = indices[0][i], indices[1][i], indices[2][i], indices[3][i]
            
            record = {
                'province_idx': int(p),
                'province': province_labels[p] if p < len(province_labels) else str(p),
                'city_idx': int(c),
                'city': city_labels[c] if c < len(city_labels) else str(c),
                'mcc_idx': int(m),
                'mcc': mcc_labels[m] if m < len(mcc_labels) else str(m),
                'day_idx': int(d),
                'day': day_labels[d] if d < len(day_labels) else str(d)
            }
            
            for query in self.QUERIES:
                record[query] = int(query_data[query][i])
            
            records.append(record)
        
        return records
    
    def to_dataframe(self):
        """
        Convert histogram directly to pandas DataFrame.
        
        This is faster than to_records() for large histograms.
        
        Returns:
            pandas DataFrame with all non-zero cells
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas required for to_dataframe()")
        
        # Use NumPy to find non-zero cells efficiently
        indices = np.where(self._has_data)
        
        if len(indices[0]) == 0:
            # Return empty DataFrame with correct columns
            columns = ['province_idx', 'province', 'city_idx', 'city', 
                      'mcc_idx', 'mcc', 'day_idx', 'day'] + self.QUERIES
            return pd.DataFrame(columns=columns)
        
        # Pre-fetch all labels
        province_labels = self.dimensions['province'].labels or [str(i) for i in range(self.shape[0])]
        city_labels = self.dimensions['city'].labels or [str(i) for i in range(self.shape[1])]
        mcc_labels = self.dimensions['mcc'].labels or [str(i) for i in range(self.shape[2])]
        day_labels = self.dimensions['day'].labels or [str(i) for i in range(self.shape[3])]
        
        # Build DataFrame columns directly from arrays
        data = {
            'province_idx': indices[0].astype(int),
            'province': [province_labels[i] if i < len(province_labels) else str(i) for i in indices[0]],
            'city_idx': indices[1].astype(int),
            'city': [city_labels[i] if i < len(city_labels) else str(i) for i in indices[1]],
            'mcc_idx': indices[2].astype(int),
            'mcc': [mcc_labels[i] if i < len(mcc_labels) else str(i) for i in indices[2]],
            'day_idx': indices[3].astype(int),
            'day': [day_labels[i] if i < len(day_labels) else str(i) for i in indices[3]],
        }
        
        # Add query data
        for query in self.QUERIES:
            data[query] = self.data[query][indices].astype(int)
        
        return pd.DataFrame(data)
    
    def summary(self) -> str:
        """Generate a summary of the histogram."""
        lines = [
            "=" * 60,
            "Transaction Histogram Summary",
            "=" * 60,
            f"Shape: {self.shape}",
            f"  Provinces: {self.shape[0]}",
            f"  Cities: {self.shape[1]}",
            f"  MCCs: {self.shape[2]}",
            f"  Days: {self.shape[3]}",
            f"Total Cells: {self.total_cells:,}",
            f"Non-Zero Cells: {self.non_zero_cells:,} ({100*self.non_zero_cells/self.total_cells:.2f}%)",
            "",
            "Query Totals:"
        ]
        
        for query in self.QUERIES:
            total = np.sum(self.data[query])
            lines.append(f"  {query}: {total:,}")
        
        lines.append("=" * 60)
        return "\n".join(lines)


class ProvinceHistogram:
    """
    Province-level histogram for top-down aggregation.
    
    Shape: (province_dim, day_dim)
    """
    
    def __init__(self, province_dim: int, day_dim: int = 30):
        self.shape = (province_dim, day_dim)
        self.data: Dict[str, np.ndarray] = {
            query: np.zeros(self.shape, dtype=np.int64)
            for query in TransactionHistogram.QUERIES
        }
    
    def aggregate_from_full(self, full_hist: TransactionHistogram) -> None:
        """Aggregate from full histogram."""
        for query in TransactionHistogram.QUERIES:
            # Sum over cities and mccs
            self.data[query] = np.sum(full_hist.data[query], axis=(1, 2))
    
    def get_query_array(self, query: str) -> np.ndarray:
        """Get array for a query."""
        return self.data[query]


class CityHistogram:
    """
    City-level histogram for top-down aggregation.
    
    Shape: (province_dim, city_dim, day_dim)
    Aggregated over MCCs.
    """
    
    def __init__(self, province_dim: int, city_dim: int, day_dim: int = 30):
        self.shape = (province_dim, city_dim, day_dim)
        self.data: Dict[str, np.ndarray] = {
            query: np.zeros(self.shape, dtype=np.int64)
            for query in TransactionHistogram.QUERIES
        }
    
    def aggregate_from_full(self, full_hist: TransactionHistogram) -> None:
        """Aggregate from full histogram."""
        for query in TransactionHistogram.QUERIES:
            # Sum over mccs only
            self.data[query] = np.sum(full_hist.data[query], axis=2)
    
    def get_query_array(self, query: str) -> np.ndarray:
        """Get array for a query."""
        return self.data[query]

