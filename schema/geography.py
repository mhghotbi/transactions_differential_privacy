"""
Geographic Hierarchy Management.

Handles the Province -> City hierarchy from city_province.csv.
"""

import csv
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass
class Province:
    """Represents a province."""
    code: int
    name: str
    cities: List[str] = field(default_factory=list)
    
    def __hash__(self):
        return hash(self.code)
    
    def __eq__(self, other):
        if isinstance(other, Province):
            return self.code == other.code
        return False


@dataclass  
class City:
    """Represents a city."""
    name: str
    province_code: int
    province_name: str
    
    def __hash__(self):
        return hash((self.name, self.province_code))
    
    def __eq__(self, other):
        if isinstance(other, City):
            return self.name == other.name and self.province_code == other.province_code
        return False


class Geography:
    """
    Manages the geographic hierarchy: Province -> City.
    
    Loads from city_province.csv with columns:
    - StateCode: Province code (int)
    - StateName: Province name (str)
    - CityName: City name (str)
    """
    
    # Special code for unknown province (cities not in city_province file)
    UNKNOWN_PROVINCE_CODE = 999
    UNKNOWN_PROVINCE_NAME = "Unknown"
    
    def __init__(self):
        self._provinces: Dict[int, Province] = {}
        self._cities: Dict[str, City] = {}
        self._city_to_province: Dict[str, int] = {}
        self._province_to_cities: Dict[int, List[str]] = {}
        self._has_unknown_province = False
    
    @classmethod
    def from_csv(cls, filepath: str, encoding: str = 'utf-8-sig') -> 'Geography':
        """
        Load geography from CSV file.
        
        Expected format:
        StateCode,StateName,CityName
        0,ProvinceName,CityName
        ...
        
        Args:
            filepath: Path to city_province.csv
            encoding: File encoding (default utf-8-sig to handle BOM)
            
        Returns:
            Geography instance
        """
        geo = cls()
        
        with open(filepath, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Handle potential BOM or whitespace in column names
                row = {k.strip().lstrip('\ufeff'): v for k, v in row.items()}
                
                province_code = int(row['StateCode'])
                province_name = row['StateName'].strip()
                city_name = row['CityName'].strip()
                
                # Add province if not exists
                if province_code not in geo._provinces:
                    geo._provinces[province_code] = Province(
                        code=province_code,
                        name=province_name,
                        cities=[]
                    )
                    geo._province_to_cities[province_code] = []
                
                # Add city
                if city_name not in geo._cities:
                    geo._cities[city_name] = City(
                        name=city_name,
                        province_code=province_code,
                        province_name=province_name
                    )
                    geo._city_to_province[city_name] = province_code
                    geo._provinces[province_code].cities.append(city_name)
                    geo._province_to_cities[province_code].append(city_name)
        
        logger.info(f"Loaded {len(geo._provinces)} provinces, {len(geo._cities)} cities")
        
        # Add "Unknown" province for cities not in the mapping file
        geo.add_unknown_province()
        
        return geo
    
    def add_unknown_province(self) -> None:
        """Add an 'Unknown' province for handling unmapped cities."""
        if self._has_unknown_province:
            return
        
        self._provinces[self.UNKNOWN_PROVINCE_CODE] = Province(
            code=self.UNKNOWN_PROVINCE_CODE,
            name=self.UNKNOWN_PROVINCE_NAME,
            cities=[]
        )
        self._province_to_cities[self.UNKNOWN_PROVINCE_CODE] = []
        self._has_unknown_province = True
        logger.info(f"Added '{self.UNKNOWN_PROVINCE_NAME}' province (code={self.UNKNOWN_PROVINCE_CODE}) for unmapped cities")
    
    @property
    def num_provinces(self) -> int:
        """Number of provinces."""
        return len(self._provinces)
    
    @property
    def num_cities(self) -> int:
        """Number of cities."""
        return len(self._cities)
    
    @property
    def provinces(self) -> List[Province]:
        """Get all provinces."""
        return list(self._provinces.values())
    
    @property
    def province_codes(self) -> List[int]:
        """Get all province codes."""
        return sorted(self._provinces.keys())
    
    @property
    def province_names(self) -> List[str]:
        """Get all province names."""
        return [p.name for p in self._provinces.values()]
    
    @property
    def cities(self) -> List[City]:
        """Get all cities."""
        return list(self._cities.values())
    
    @property
    def city_names(self) -> List[str]:
        """Get all city names."""
        return sorted(self._cities.keys())
    
    def get_province(self, code: int) -> Optional[Province]:
        """Get province by code."""
        return self._provinces.get(code)
    
    def get_province_by_name(self, name: str) -> Optional[Province]:
        """Get province by name."""
        for p in self._provinces.values():
            if p.name == name:
                return p
        return None
    
    def get_city(self, name: str) -> Optional[City]:
        """Get city by name."""
        return self._cities.get(name)
    
    def get_province_for_city(self, city_name: str) -> Optional[int]:
        """Get province code for a city."""
        return self._city_to_province.get(city_name)
    
    def get_province_name_for_city(self, city_name: str) -> Optional[str]:
        """Get province name for a city."""
        province_code = self._city_to_province.get(city_name)
        if province_code is not None:
            province = self._provinces.get(province_code)
            if province:
                return province.name
        return None
    
    def get_cities_in_province(self, province_code: int) -> List[str]:
        """Get all cities in a province."""
        return self._province_to_cities.get(province_code, [])
    
    def get_hierarchy(self) -> Dict[str, List[str]]:
        """
        Get the full hierarchy as a dictionary.
        
        Returns:
            Dict mapping province_name -> list of city_names
        """
        hierarchy = {}
        for province in self._provinces.values():
            hierarchy[province.name] = sorted(province.cities)
        return hierarchy
    
    def city_to_province_broadcast(self) -> Dict[str, Tuple[int, str]]:
        """
        Get a dictionary suitable for Spark broadcast.
        
        Returns:
            Dict mapping city_name -> (province_code, province_name)
        """
        result = {}
        for city_name, city in self._cities.items():
            result[city_name] = (city.province_code, city.province_name)
        return result
    
    def validate_city(self, city_name: str) -> bool:
        """Check if a city exists in the hierarchy."""
        return city_name in self._cities
    
    def get_unknown_cities(self, city_names: Set[str]) -> Set[str]:
        """
        Find cities that are not in the hierarchy.
        
        Args:
            city_names: Set of city names to check
            
        Returns:
            Set of city names not found in hierarchy
        """
        known_cities = set(self._cities.keys())
        return city_names - known_cities
    
    def summary(self) -> str:
        """Generate a summary of the geography."""
        lines = [
            "=" * 60,
            "Geography Summary",
            "=" * 60,
            f"Total Provinces: {self.num_provinces}",
            f"Total Cities: {self.num_cities}",
            "",
            "Provinces:",
        ]
        
        for code in sorted(self._provinces.keys()):
            province = self._provinces[code]
            lines.append(f"  [{code:2d}] {province.name}: {len(province.cities)} cities")
        
        lines.append("=" * 60)
        return "\n".join(lines)

