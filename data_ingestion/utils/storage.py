"""
Utility functions for data storage and processing
"""
import json
import csv
import os
import re
import logging
from typing import List, Dict, Any
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)


class DataStorage:
    """Handle storage of scraped data to various formats"""
    
    def __init__(self, base_dir: str = './data'):
        """
        Initialize data storage
        
        Args:
            base_dir: Base directory for data storage
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / 'raw'
        self.processed_dir = self.base_dir / 'processed'
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Data storage initialized at: {self.base_dir}")
    
    def save_movies_to_json(
        self,
        movies: List[Any],
        filename: str,
        raw: bool = True
    ) -> str:
        """
        Save movies to JSON file
        
        Args:
            movies: List of Movie objects
            filename: Output filename
            raw: Save to raw directory if True, processed if False
            
        Returns:
            Path to saved file
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        # Convert movies to dictionaries
        movie_dicts = [movie.to_dict() for movie in movies]
        
        # Save to JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(movie_dicts, f, indent=2, default=self._json_serializer)
        
        logger.info(f"Saved {len(movies)} movies to {filepath}")
        return str(filepath)
    
    def save_awards_to_json(
        self,
        awards: List[Any],
        filename: str,
        raw: bool = True
    ) -> str:
        """
        Save awards to JSON file
        
        Args:
            awards: List of Award objects
            filename: Output filename
            raw: Save to raw directory if True, processed if False
            
        Returns:
            Path to saved file
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        # Convert awards to dictionaries
        award_dicts = [award.to_dict() for award in awards]
        
        # Save to JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(award_dicts, f, indent=2, default=self._json_serializer)
        
        logger.info(f"Saved {len(awards)} awards to {filepath}")
        return str(filepath)
    
    def save_movies_to_csv(
        self,
        movies: List[Any],
        filename: str,
        raw: bool = True
    ) -> str:
        """
        Save movies to CSV file
        
        Args:
            movies: List of Movie objects
            filename: Output filename
            raw: Save to raw directory if True, processed if False
            
        Returns:
            Path to saved file
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        if not movies:
            logger.warning("No movies to save")
            return str(filepath)
        
        # Convert movies to dictionaries and flatten
        movie_dicts = [self._flatten_dict(movie.to_dict()) for movie in movies]
        
        # Get all unique keys
        all_keys = set()
        for movie_dict in movie_dicts:
            all_keys.update(movie_dict.keys())
        
        # Write CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
            writer.writeheader()
            writer.writerows(movie_dicts)
        
        logger.info(f"Saved {len(movies)} movies to {filepath}")
        return str(filepath)
    
    def save_awards_to_csv(
        self,
        awards: List[Any],
        filename: str,
        raw: bool = True
    ) -> str:
        """
        Save awards to CSV file
        
        Args:
            awards: List of Award objects
            filename: Output filename
            raw: Save to raw directory if True, processed if False
            
        Returns:
            Path to saved file
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        if not awards:
            logger.warning("No awards to save")
            return str(filepath)
        
        # Convert awards to dictionaries
        award_dicts = [award.to_dict() for award in awards]
        
        # Get all unique keys
        all_keys = set()
        for award_dict in award_dicts:
            all_keys.update(award_dict.keys())
        
        # Write CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
            writer.writeheader()
            writer.writerows(award_dicts)
        
        logger.info(f"Saved {len(awards)} awards to {filepath}")
        return str(filepath)
    
    def load_movies_from_json(self, filename: str, raw: bool = True) -> List[Dict]:
        """
        Load movies from JSON file
        
        Args:
            filename: Input filename
            raw: Load from raw directory if True, processed if False
            
        Returns:
            List of movie dictionaries
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        with open(filepath, 'r', encoding='utf-8') as f:
            movies = json.load(f)
        
        logger.info(f"Loaded {len(movies)} movies from {filepath}")
        return movies
    
    def load_awards_from_json(self, filename: str, raw: bool = True) -> List[Dict]:
        """
        Load awards from JSON file
        
        Args:
            filename: Input filename
            raw: Load from raw directory if True, processed if False
            
        Returns:
            List of award dictionaries
        """
        directory = self.raw_dir if raw else self.processed_dir
        filepath = directory / filename
        
        with open(filepath, 'r', encoding='utf-8') as f:
            awards = json.load(f)
        
        logger.info(f"Loaded {len(awards)} awards from {filepath}")
        return awards
    
    @staticmethod
    def _json_serializer(obj):
        """JSON serializer for objects not serializable by default"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    @staticmethod
    def _flatten_dict(d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(DataStorage._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to JSON strings for CSV
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        
        return dict(items)


class DataMatcher:
    """Match movies between different data sources"""
    
    @staticmethod
    def match_movie_to_award(
        movie_title: str,
        award_movie_title: str,
        threshold: float = 0.8
    ) -> bool:
        """
        Match movie titles using simple string similarity
        
        Args:
            movie_title: Movie title from TMDb
            award_movie_title: Movie title from awards data
            threshold: Similarity threshold (0-1)
            
        Returns:
            True if titles match above threshold
        """
        # Simple matching - normalize titles
        title1 = DataMatcher._normalize_title(movie_title)
        title2 = DataMatcher._normalize_title(award_movie_title)
        
        # Exact match
        if title1 == title2:
            return True
        
        # Check if one is substring of other
        if title1 in title2 or title2 in title1:
            return True
        
        # Calculate similarity (simple Jaccard similarity)
        words1 = set(title1.split())
        words2 = set(title2.split())
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        if not union:
            return False
        
        similarity = len(intersection) / len(union)
        return similarity >= threshold
    
    @staticmethod
    def _normalize_title(title: str) -> str:
        """Normalize movie title for matching"""
        # Convert to lowercase
        title = title.lower()
        
        # Remove common articles
        title = re.sub(r'\b(the|a|an)\b', '', title)
        
        # Remove punctuation
        title = re.sub(r'[^\w\s]', '', title)
        
        # Remove extra whitespace
        title = ' '.join(title.split())
        
        return title.strip()
    
    @staticmethod
    def enrich_awards_with_movie_data(
        awards: List[Dict],
        movies: List[Dict]
    ) -> List[Dict]:
        """
        Enrich award data with movie metadata
        
        Args:
            awards: List of award dictionaries
            movies: List of movie dictionaries
            
        Returns:
            Enriched award dictionaries
        """
        # Create movie lookup by title
        movie_lookup = {
            DataMatcher._normalize_title(m['title']): m
            for m in movies
        }
        
        enriched_awards = []
        
        for award in awards:
            enriched = award.copy()
            
            if award.get('movie_title'):
                normalized_title = DataMatcher._normalize_title(award['movie_title'])
                
                # Try exact match
                if normalized_title in movie_lookup:
                    movie = movie_lookup[normalized_title]
                    enriched['movie_tmdb_id'] = movie.get('tmdb_id')
                    enriched['movie_imdb_id'] = movie.get('imdb_id')
                else:
                    # Try fuzzy match
                    for movie_title, movie in movie_lookup.items():
                        if DataMatcher.match_movie_to_award(
                            movie['title'],
                            award['movie_title']
                        ):
                            enriched['movie_tmdb_id'] = movie.get('tmdb_id')
                            enriched['movie_imdb_id'] = movie.get('imdb_id')
                            break
            
            enriched_awards.append(enriched)
        
        return enriched_awards


if __name__ == "__main__":
    # Example usage
    import logging
    from models.data_models import Movie, Award
    from datetime import date
    
    logging.basicConfig(level=logging.INFO)
    
    # Create storage
    storage = DataStorage('./data')
    
    # Example movies
    movies = [
        Movie(
            tmdb_id=550,
            title="Fight Club",
            release_date=date(1999, 10, 15),
            director="David Fincher"
        ),
        Movie(
            tmdb_id=680,
            title="Pulp Fiction",
            release_date=date(1994, 10, 14),
            director="Quentin Tarantino"
        )
    ]
    
    # Example awards
    awards = [
        Award(
            award_name="Academy Awards",
            ceremony_year=2024,
            category="Best Picture",
            movie_title="Oppenheimer",
            won=True
        )
    ]
    
    # Save to JSON
    storage.save_movies_to_json(movies, 'movies_test.json')
    storage.save_awards_to_json(awards, 'awards_test.json')
    
    # Save to CSV
    storage.save_movies_to_csv(movies, 'movies_test.csv')
    storage.save_awards_to_csv(awards, 'awards_test.csv')
    
    print("\nData saved successfully!")
