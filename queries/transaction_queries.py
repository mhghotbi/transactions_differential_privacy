"""
Transaction Query Definitions.

Defines the four main queries for transaction data:
1. Transaction count
2. Unique cards count
3. Unique acceptors count
4. Total amount (sum)
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from fractions import Fraction

import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class QuerySpec:
    """Specification for a query."""
    name: str
    description: str
    sensitivity: float
    is_counting: bool
    aggregation: str  # 'count', 'count_distinct', 'sum'
    
    def __repr__(self) -> str:
        return f"QuerySpec({self.name}, sens={self.sensitivity})"


class TransactionCountQuery:
    """
    Query for counting transactions.
    
    Counts the number of transactions in each cell.
    L2 Sensitivity: K (bounded contribution per card-cell)
    
    Note: Sensitivity = K where K is the max transactions per card per cell.
    K is computed using IQR method during preprocessing.
    """
    
    # Default spec with K=1 (will be updated with actual K)
    spec = QuerySpec(
        name='transaction_count',
        description='Number of transactions',
        sensitivity=1.0,  # Default, actual value depends on computed K
        is_counting=True,
        aggregation='count'
    )
    
    @staticmethod
    def compute(transaction_ids: List[str]) -> int:
        """Compute transaction count."""
        return len(transaction_ids)
    
    @staticmethod
    def sensitivity(contribution_bound: int = 1) -> float:
        """
        L2 sensitivity for this query.
        
        Args:
            contribution_bound: Max transactions per card per cell (K)
            
        Returns:
            Sensitivity = K (one card can contribute at most K transactions)
        """
        return float(contribution_bound)


class UniqueCardsQuery:
    """
    Query for counting unique card numbers.
    
    Counts distinct card numbers in each cell.
    L2 Sensitivity: 1 (one card can only contribute 1 to the count)
    """
    
    spec = QuerySpec(
        name='unique_cards',
        description='Number of unique card numbers',
        sensitivity=1.0,
        is_counting=True,
        aggregation='count_distinct'
    )
    
    @staticmethod
    def compute(card_numbers: List[str]) -> int:
        """Compute unique cards count."""
        return len(set(card_numbers))
    
    @staticmethod
    def sensitivity() -> float:
        """L2 sensitivity for this query."""
        return 1.0


class UniqueAcceptorsQuery:
    """
    Query for counting unique acceptors.
    
    Counts distinct acceptor IDs in each cell.
    L2 Sensitivity: 1 (one acceptor can only contribute 1 to the count)
    """
    
    spec = QuerySpec(
        name='unique_acceptors',
        description='Number of unique acceptors (merchants)',
        sensitivity=1.0,
        is_counting=True,
        aggregation='count_distinct'
    )
    
    @staticmethod
    def compute(acceptor_ids: List[str]) -> int:
        """Compute unique acceptors count."""
        return len(set(acceptor_ids))
    
    @staticmethod
    def sensitivity() -> float:
        """L2 sensitivity for this query."""
        return 1.0


class TotalAmountQuery:
    """
    Query for summing transaction amounts.
    
    Sums the (winsorized) transaction amounts in each cell.
    L2 Sensitivity: winsorize_cap (maximum contribution of one transaction)
    
    Note: With winsorization, each transaction contributes at most
    the winsorize cap to the sum.
    """
    
    spec = QuerySpec(
        name='total_amount',
        description='Sum of transaction amounts (winsorized)',
        sensitivity=1.0,  # After normalization by cap
        is_counting=False,
        aggregation='sum'
    )
    
    def __init__(self, winsorize_cap: float = 1.0):
        """
        Initialize with winsorization cap.
        
        Args:
            winsorize_cap: Maximum value for any single transaction
        """
        self.winsorize_cap = winsorize_cap
    
    def compute(self, amounts: List[float]) -> float:
        """Compute total amount with winsorization."""
        winsorized = [min(a, self.winsorize_cap) for a in amounts]
        return sum(winsorized)
    
    def sensitivity(self) -> float:
        """L2 sensitivity for this query."""
        return self.winsorize_cap


class TransactionWorkload:
    """
    Complete workload of queries for transaction data.
    
    Combines all four queries with budget allocation.
    """
    
    QUERIES = [
        TransactionCountQuery,
        UniqueCardsQuery,
        UniqueAcceptorsQuery,
        TotalAmountQuery
    ]
    
    QUERY_NAMES = [
        'transaction_count',
        'unique_cards',
        'unique_acceptors',
        'total_amount'
    ]
    
    def __init__(
        self,
        budget_allocation: Optional[Dict[str, float]] = None,
        winsorize_cap: float = 1.0
    ):
        """
        Initialize workload.
        
        Args:
            budget_allocation: Dict mapping query names to budget proportions
            winsorize_cap: Cap for amount winsorization
        """
        self.budget_allocation = budget_allocation or {
            'transaction_count': 0.25,
            'unique_cards': 0.25,
            'unique_acceptors': 0.25,
            'total_amount': 0.25
        }
        
        self.winsorize_cap = winsorize_cap
        
        # Validate allocations sum to 1
        total = sum(self.budget_allocation.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Budget allocations must sum to 1.0, got {total}")
    
    def get_query_specs(self) -> Dict[str, QuerySpec]:
        """Get specifications for all queries."""
        specs = {
            'transaction_count': TransactionCountQuery.spec,
            'unique_cards': UniqueCardsQuery.spec,
            'unique_acceptors': UniqueAcceptorsQuery.spec,
            'total_amount': QuerySpec(
                name='total_amount',
                description='Sum of transaction amounts (winsorized)',
                sensitivity=self.winsorize_cap,
                is_counting=False,
                aggregation='sum'
            )
        }
        return specs
    
    def get_sensitivities(self, contribution_bound: int = 1) -> Dict[str, float]:
        """
        Get L2 sensitivities for all queries.
        
        Args:
            contribution_bound: Max transactions per card per cell (K)
            
        Returns:
            Dict mapping query names to sensitivities
        """
        return {
            'transaction_count': float(contribution_bound),  # K transactions per card-cell
            'unique_cards': 1.0,  # Each card contributes at most 1
            'unique_acceptors': 1.0,  # Each card affects at most 1 acceptor per cell
            'total_amount': self.winsorize_cap
        }
    
    def compute_sigmas(
        self,
        total_rho: Fraction,
        geo_level: str = 'city',
        geo_split: float = 0.8
    ) -> Dict[str, float]:
        """
        Compute sigma values for each query given budget.
        
        Args:
            total_rho: Total privacy budget (rho for zCDP)
            geo_level: Geographic level for budget split
            geo_split: Proportion of budget for this level
            
        Returns:
            Dict mapping query names to sigma values
        """
        import math
        
        sigmas = {}
        sensitivities = self.get_sensitivities()
        
        for query_name, budget_prop in self.budget_allocation.items():
            # rho for this query
            query_rho = float(total_rho) * geo_split * budget_prop
            
            if query_rho <= 0:
                sigmas[query_name] = float('inf')
            else:
                # sigma^2 = sensitivity^2 / (2 * rho)
                sensitivity = sensitivities[query_name]
                sigma_sq = (sensitivity ** 2) / (2 * query_rho)
                sigmas[query_name] = math.sqrt(sigma_sq)
        
        return sigmas
    
    def summary(self) -> str:
        """Generate workload summary."""
        lines = [
            "=" * 60,
            "Transaction Workload Summary",
            "=" * 60,
            "",
            "Queries:",
        ]
        
        specs = self.get_query_specs()
        for name, spec in specs.items():
            budget = self.budget_allocation[name]
            lines.append(f"  {name}:")
            lines.append(f"    Description: {spec.description}")
            lines.append(f"    Sensitivity: {spec.sensitivity}")
            lines.append(f"    Budget Share: {budget:.0%}")
            lines.append("")
        
        lines.append("=" * 60)
        return "\n".join(lines)

