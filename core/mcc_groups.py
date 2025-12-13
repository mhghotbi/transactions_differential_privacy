"""
MCC Grouping for Stratified Sensitivity.

Groups MCCs by order of magnitude of typical transaction amounts,
enabling per-group sensitivity calibration for the total_amount query.

Under parallel composition (disjoint MCC groups), each group gets
the full privacy budget without additive cost.
"""

import logging
import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

import numpy as np

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


logger = logging.getLogger(__name__)


@dataclass
class MCCGroupInfo:
    """Information about an MCC group."""
    group_id: int
    name: str
    mcc_codes: List[str]
    median_amount: float
    cap: float  # Winsorization cap (99th percentile)
    num_mccs: int
    
    def __repr__(self):
        return f"MCCGroup({self.name}: {self.num_mccs} MCCs, median={self.median_amount:,.0f}, cap={self.cap:,.0f})"


@dataclass 
class MCCGroupingResult:
    """Result of MCC grouping computation."""
    mcc_to_group: Dict[str, int]  # MCC code -> group_id
    group_info: Dict[int, MCCGroupInfo]  # group_id -> info
    boundaries: List[float]  # Log10 boundaries between groups
    
    @property
    def num_groups(self) -> int:
        return len(self.group_info)
    
    def get_cap(self, mcc_code: str) -> float:
        """Get winsorization cap for an MCC code."""
        group_id = self.mcc_to_group.get(mcc_code)
        if group_id is None:
            # Unknown MCC - use highest cap (conservative)
            return max(g.cap for g in self.group_info.values())
        return self.group_info[group_id].cap
    
    def get_group_id(self, mcc_code: str) -> int:
        """Get group ID for an MCC code."""
        return self.mcc_to_group.get(mcc_code, 0)
    
    def summary(self) -> str:
        """Generate summary of MCC grouping."""
        lines = [
            "=" * 70,
            "MCC Grouping Summary",
            "=" * 70,
            f"Number of groups: {self.num_groups}",
            f"Log10 boundaries: {[f'{b:.1f}' for b in self.boundaries]}",
            "",
            "Groups:",
        ]
        
        for gid in sorted(self.group_info.keys()):
            info = self.group_info[gid]
            lines.append(
                f"  Group {gid} ({info.name}): "
                f"{info.num_mccs} MCCs, "
                f"median={info.median_amount:,.0f}, "
                f"cap={info.cap:,.0f}"
            )
        
        lines.append("=" * 70)
        return "\n".join(lines)


# Default group names by order of magnitude
DEFAULT_GROUP_NAMES = {
    0: "micro",      # < 1M
    1: "small",      # 1M - 10M
    2: "medium",     # 10M - 100M
    3: "large",      # 100M - 1B
    4: "very_large", # > 1B
}


def compute_mcc_groups_spark(
    df: DataFrame,
    mcc_col: str = 'mcc',
    amount_col: str = 'amount',
    num_groups: int = 5,
    cap_percentile: float = 99.0
) -> MCCGroupingResult:
    """
    Compute MCC groups from Spark DataFrame.
    
    Groups MCCs by log10 of median transaction amount.
    
    Args:
        df: Transaction DataFrame
        mcc_col: Column name for MCC code
        amount_col: Column name for transaction amount
        num_groups: Target number of groups (actual may be less if data sparse)
        cap_percentile: Percentile for computing per-group cap
        
    Returns:
        MCCGroupingResult with grouping information
    """
    if not HAS_SPARK:
        raise RuntimeError("Spark not available")
    
    logger.info(f"Computing MCC groups (target: {num_groups} groups)")
    
    # Step 1: Compute median and percentile per MCC
    mcc_stats = df.groupBy(mcc_col).agg(
        F.expr(f'percentile_approx({amount_col}, 0.5)').alias('median'),
        F.expr(f'percentile_approx({amount_col}, {cap_percentile/100})').alias('cap'),
        F.count('*').alias('tx_count')
    ).collect()
    
    if not mcc_stats:
        logger.warning("No MCC statistics computed - empty data?")
        return MCCGroupingResult(
            mcc_to_group={},
            group_info={},
            boundaries=[]
        )
    
    # Step 2: Build MCC info
    mcc_data = {}
    for row in mcc_stats:
        mcc_code = str(row[mcc_col])
        median = float(row['median']) if row['median'] else 1.0
        cap = float(row['cap']) if row['cap'] else median
        mcc_data[mcc_code] = {
            'median': max(median, 1.0),  # Avoid log(0)
            'cap': max(cap, 1.0),
            'tx_count': row['tx_count']
        }
    
    logger.info(f"Computed statistics for {len(mcc_data)} MCCs")
    
    # Step 3: Compute log10 of medians and determine boundaries
    log_medians = [math.log10(d['median']) for d in mcc_data.values()]
    min_log = min(log_medians)
    max_log = max(log_medians)
    
    logger.info(f"Log10 median range: {min_log:.2f} to {max_log:.2f}")
    
    # Create boundaries using percentiles of log medians
    if num_groups <= 1:
        boundaries = []
    else:
        percentiles = np.linspace(0, 100, num_groups + 1)[1:-1]
        boundaries = list(np.percentile(log_medians, percentiles))
    
    logger.info(f"Group boundaries (log10): {boundaries}")
    
    # Step 4: Assign MCCs to groups
    def get_group(log_median):
        for i, boundary in enumerate(boundaries):
            if log_median < boundary:
                return i
        return len(boundaries)
    
    mcc_to_group = {}
    groups = {}  # group_id -> list of (mcc_code, median, cap)
    
    for mcc_code, data in mcc_data.items():
        log_median = math.log10(data['median'])
        group_id = get_group(log_median)
        mcc_to_group[mcc_code] = group_id
        
        if group_id not in groups:
            groups[group_id] = []
        groups[group_id].append((mcc_code, data['median'], data['cap']))
    
    # Step 5: Compute per-group statistics
    group_info = {}
    for group_id, mccs in groups.items():
        mcc_codes = [m[0] for m in mccs]
        medians = [m[1] for m in mccs]
        caps = [m[2] for m in mccs]
        
        # Group cap is max of individual MCC caps in the group
        group_cap = max(caps)
        group_median = np.median(medians)
        
        group_name = DEFAULT_GROUP_NAMES.get(group_id, f"group_{group_id}")
        
        group_info[group_id] = MCCGroupInfo(
            group_id=group_id,
            name=group_name,
            mcc_codes=mcc_codes,
            median_amount=group_median,
            cap=group_cap,
            num_mccs=len(mcc_codes)
        )
    
    result = MCCGroupingResult(
        mcc_to_group=mcc_to_group,
        group_info=group_info,
        boundaries=boundaries
    )
    
    logger.info(result.summary())
    
    return result


def compute_mcc_groups_multifactor(
    df: DataFrame,
    mcc_col: str = 'mcc',
    amount_col: str = 'amount',
    date_col: str = 'transaction_date',
    num_groups: int = 10,
    adaptive: bool = False,
    min_groups: int = 3,
    max_groups: int = 15,
    min_mccs_per_group: int = 3,
    cap_percentile: float = 99.0
) -> MCCGroupingResult:
    """
    Compute MCC groups using multiple factors with hierarchical clustering.
    
    Uses SPARK for feature extraction, then hierarchical clustering on small aggregated data.
    
    Features:
    1. Transaction count - volume of transactions per MCC
    2. Weekend ratio - weekday vs weekend transaction pattern
    3. Total volume - sum of all transaction amounts
    4. Average amount - mean transaction value
    5. Median amount - typical transaction value (robust to outliers)
    
    Args:
        df: Transaction DataFrame (Spark)
        mcc_col: Column name for MCC code
        amount_col: Column name for transaction amount
        date_col: Column name for transaction date
        num_groups: Target number of groups (used if not adaptive)
        adaptive: If True, automatically find optimal number of groups
        min_groups: Minimum groups (for adaptive mode)
        max_groups: Maximum groups (for adaptive mode)
        min_mccs_per_group: Minimum MCCs per group (validation/merging)
        cap_percentile: Percentile for computing per-group cap
        
    Returns:
        MCCGroupingResult with grouping information
    """
    if not HAS_SPARK:
        raise RuntimeError("Spark not available")
    
    logger.info(f"Computing MCC groups with multi-factor features (target: {num_groups} groups)")
    logger.info(f"Adaptive mode: {adaptive}, min_groups: {min_groups}, max_groups: {max_groups}")
    
    # Step 1: Feature extraction using Spark
    logger.info("Extracting features per MCC using Spark...")
    
    mcc_features = df.groupBy(mcc_col).agg(
        F.count('*').alias('tx_count'),
        F.sum(amount_col).alias('total_volume'),
        F.avg(amount_col).alias('avg_amount'),
        F.expr(f'percentile_approx({amount_col}, 0.5)').alias('median_amount'),
        F.expr(f'percentile_approx({amount_col}, {cap_percentile/100})').alias('cap'),
        # Weekend transactions (Sunday=1, Saturday=7)
        F.sum(F.when(F.dayofweek(F.col(date_col)).isin([1, 7]), 1).otherwise(0)).alias('weekend_tx')
    ).collect()
    
    if not mcc_features:
        logger.warning("No MCC features computed - empty data?")
        return MCCGroupingResult(mcc_to_group={}, group_info={}, boundaries=[])
    
    logger.info(f"Extracted features for {len(mcc_features)} MCCs")
    
    # Step 2: Build feature matrix (small data, can use pandas/numpy)
    import pandas as pd
    from scipy.cluster.hierarchy import linkage, fcluster
    from scipy.spatial.distance import squareform
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import silhouette_score
    
    mcc_codes = []
    features = []
    mcc_caps = {}
    
    for row in mcc_features:
        mcc_code = str(row[mcc_col])
        tx_count = int(row['tx_count'])
        total_volume = float(row['total_volume']) if row['total_volume'] else 0.0
        avg_amount = float(row['avg_amount']) if row['avg_amount'] else 1.0
        median_amount = float(row['median_amount']) if row['median_amount'] else 1.0
        cap = float(row['cap']) if row['cap'] else median_amount
        weekend_tx = int(row['weekend_tx'])
        
        # Compute weekend ratio
        weekend_ratio = weekend_tx / tx_count if tx_count > 0 else 0.0
        
        # Feature vector: [log(tx_count), log(total_volume), log(avg), log(median), weekend_ratio]
        feature_vec = [
            math.log10(max(tx_count, 1)),
            math.log10(max(total_volume, 1)),
            math.log10(max(avg_amount, 1)),
            math.log10(max(median_amount, 1)),
            weekend_ratio
        ]
        
        mcc_codes.append(mcc_code)
        features.append(feature_vec)
        mcc_caps[mcc_code] = max(cap, 1.0)
    
    features_array = np.array(features)
    logger.info(f"Feature matrix shape: {features_array.shape}")
    
    # Step 3: Normalize features
    scaler = StandardScaler()
    features_normalized = scaler.fit_transform(features_array)
    
    # Step 4: Hierarchical clustering with Ward linkage
    logger.info("Performing hierarchical clustering (Ward linkage)...")
    linkage_matrix = linkage(features_normalized, method='ward')
    
    # Step 5: Determine optimal number of groups
    if adaptive and len(mcc_codes) >= min_groups:
        logger.info(f"Finding optimal number of groups ({min_groups}-{max_groups})...")
        best_score = -1
        best_n_groups = num_groups
        
        for n in range(min_groups, min(max_groups + 1, len(mcc_codes))):
            labels = fcluster(linkage_matrix, n, criterion='maxclust')
            
            # Check minimum group size constraint
            unique, counts = np.unique(labels, return_counts=True)
            if np.any(counts < min_mccs_per_group):
                continue
            
            # Compute silhouette score
            if len(unique) > 1:
                score = silhouette_score(features_normalized, labels)
                logger.info(f"  n_groups={n}: silhouette={score:.3f}")
                
                if score > best_score:
                    best_score = score
                    best_n_groups = n
        
        num_groups = best_n_groups
        logger.info(f"Optimal number of groups: {num_groups} (silhouette={best_score:.3f})")
    
    # Step 6: Cut dendrogram to get final groups
    cluster_labels = fcluster(linkage_matrix, num_groups, criterion='maxclust')
    
    # Step 7: Merge small groups if needed
    unique_labels, label_counts = np.unique(cluster_labels, return_counts=True)
    small_groups = unique_labels[label_counts < min_mccs_per_group]
    
    if len(small_groups) > 0:
        logger.info(f"Merging {len(small_groups)} small groups...")
        
        for small_label in small_groups:
            # Find indices of MCCs in this small group
            small_indices = np.where(cluster_labels == small_label)[0]
            
            # Find nearest larger group based on feature distance
            min_dist = np.inf
            nearest_label = None
            
            for large_label in unique_labels:
                if large_label in small_groups or large_label == small_label:
                    continue
                
                large_indices = np.where(cluster_labels == large_label)[0]
                
                # Compute average distance between groups
                for si in small_indices:
                    for li in large_indices:
                        dist = np.linalg.norm(features_normalized[si] - features_normalized[li])
                        if dist < min_dist:
                            min_dist = dist
                            nearest_label = large_label
            
            if nearest_label is not None:
                cluster_labels[small_indices] = nearest_label
        
        # Re-label to be contiguous
        unique_labels = np.unique(cluster_labels)
        label_map = {old: new for new, old in enumerate(unique_labels)}
        cluster_labels = np.array([label_map[label] for label in cluster_labels])
    
    # Step 8: Build MCC to group mapping
    mcc_to_group = {}
    groups = {}
    
    for mcc_code, group_id in zip(mcc_codes, cluster_labels):
        group_id = int(group_id)
        mcc_to_group[mcc_code] = group_id
        
        if group_id not in groups:
            groups[group_id] = []
        
        # Get MCC stats
        idx = mcc_codes.index(mcc_code)
        median = 10 ** features_array[idx, 3]  # Reverse log10 of median
        cap = mcc_caps[mcc_code]
        
        groups[group_id].append((mcc_code, median, cap))
    
    # Step 9: Compute per-group statistics
    group_info = {}
    for group_id, mccs in groups.items():
        mcc_codes_in_group = [m[0] for m in mccs]
        medians = [m[1] for m in mccs]
        caps = [m[2] for m in mccs]
        
        # Group cap is 95th percentile (less conservative than max)
        group_cap = np.percentile(caps, 95) if len(caps) > 1 else caps[0]
        group_median = np.median(medians)
        
        group_name = DEFAULT_GROUP_NAMES.get(group_id, f"group_{group_id}")
        
        group_info[group_id] = MCCGroupInfo(
            group_id=group_id,
            name=group_name,
            mcc_codes=mcc_codes_in_group,
            median_amount=group_median,
            cap=group_cap,
            num_mccs=len(mcc_codes_in_group)
        )
    
    result = MCCGroupingResult(
        mcc_to_group=mcc_to_group,
        group_info=group_info,
        boundaries=[]  # No explicit boundaries in hierarchical clustering
    )
    
    logger.info(result.summary())
    
    return result


def compute_mcc_groups_pandas(
    df,
    mcc_col: str = 'mcc',
    amount_col: str = 'amount',
    num_groups: int = 5,
    cap_percentile: float = 99.0
) -> MCCGroupingResult:
    """
    Compute MCC groups from pandas DataFrame.
    
    Alternative implementation for non-Spark environments.
    """
    import pandas as pd
    
    logger.info(f"Computing MCC groups from pandas (target: {num_groups} groups)")
    
    # Compute statistics per MCC
    mcc_stats = df.groupby(mcc_col)[amount_col].agg([
        ('median', 'median'),
        ('cap', lambda x: np.percentile(x, cap_percentile)),
        ('count', 'count')
    ]).reset_index()
    
    if mcc_stats.empty:
        return MCCGroupingResult(mcc_to_group={}, group_info={}, boundaries=[])
    
    # Build MCC data
    mcc_data = {}
    for _, row in mcc_stats.iterrows():
        mcc_code = str(row[mcc_col])
        mcc_data[mcc_code] = {
            'median': max(float(row['median']), 1.0),
            'cap': max(float(row['cap']), 1.0),
            'tx_count': int(row['count'])
        }
    
    # Compute boundaries
    log_medians = [math.log10(d['median']) for d in mcc_data.values()]
    
    if num_groups <= 1:
        boundaries = []
    else:
        percentiles = np.linspace(0, 100, num_groups + 1)[1:-1]
        boundaries = list(np.percentile(log_medians, percentiles))
    
    # Assign groups
    def get_group(log_median):
        for i, boundary in enumerate(boundaries):
            if log_median < boundary:
                return i
        return len(boundaries)
    
    mcc_to_group = {}
    groups = {}
    
    for mcc_code, data in mcc_data.items():
        log_median = math.log10(data['median'])
        group_id = get_group(log_median)
        mcc_to_group[mcc_code] = group_id
        
        if group_id not in groups:
            groups[group_id] = []
        groups[group_id].append((mcc_code, data['median'], data['cap']))
    
    # Build group info
    group_info = {}
    for group_id, mccs in groups.items():
        mcc_codes = [m[0] for m in mccs]
        medians = [m[1] for m in mccs]
        caps = [m[2] for m in mccs]
        
        group_info[group_id] = MCCGroupInfo(
            group_id=group_id,
            name=DEFAULT_GROUP_NAMES.get(group_id, f"group_{group_id}"),
            mcc_codes=mcc_codes,
            median_amount=float(np.median(medians)),
            cap=float(max(caps)),
            num_mccs=len(mcc_codes)
        )
    
    return MCCGroupingResult(
        mcc_to_group=mcc_to_group,
        group_info=group_info,
        boundaries=boundaries
    )

