"""
Data models for movies and awards
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import date, datetime


@dataclass
class Movie:
    """Movie data model"""
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    title: str = ""
    original_title: str = ""
    release_date: Optional[date] = None
    runtime_minutes: Optional[int] = None
    budget: Optional[int] = None
    revenue: Optional[int] = None
    original_language: str = ""
    overview: str = ""
    tagline: str = ""
    status: str = ""  # Released, Post Production, etc.
    
    # Collections
    genres: List[str] = field(default_factory=list)
    production_countries: List[str] = field(default_factory=list)
    production_companies: List[str] = field(default_factory=list)
    spoken_languages: List[str] = field(default_factory=list)
    
    # Ratings
    tmdb_rating: Optional[float] = None
    tmdb_vote_count: Optional[int] = None
    imdb_rating: Optional[float] = None
    imdb_vote_count: Optional[int] = None
    metacritic_score: Optional[int] = None
    
    # People
    director: Optional[str] = None
    cast: List[Dict[str, str]] = field(default_factory=list)
    crew: List[Dict[str, str]] = field(default_factory=list)
    
    # Metadata
    poster_path: Optional[str] = None
    backdrop_path: Optional[str] = None
    homepage: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'tmdb_id': self.tmdb_id,
            'imdb_id': self.imdb_id,
            'title': self.title,
            'original_title': self.original_title,
            'release_date': self.release_date.isoformat() if self.release_date else None,
            'runtime_minutes': self.runtime_minutes,
            'budget': self.budget,
            'revenue': self.revenue,
            'original_language': self.original_language,
            'overview': self.overview,
            'tagline': self.tagline,
            'status': self.status,
            'genres': self.genres,
            'production_countries': self.production_countries,
            'production_companies': self.production_companies,
            'spoken_languages': self.spoken_languages,
            'tmdb_rating': self.tmdb_rating,
            'tmdb_vote_count': self.tmdb_vote_count,
            'imdb_rating': self.imdb_rating,
            'imdb_vote_count': self.imdb_vote_count,
            'metacritic_score': self.metacritic_score,
            'director': self.director,
            'cast': self.cast,
            'crew': self.crew,
            'poster_path': self.poster_path,
            'backdrop_path': self.backdrop_path,
            'homepage': self.homepage
        }


@dataclass
class Award:
    """Award nomination/win data model"""
    award_name: str = ""  # e.g., "Academy Awards", "Golden Globes"
    ceremony_year: Optional[int] = None
    ceremony_number: Optional[int] = None  # e.g., 95th Academy Awards
    category: str = ""  # e.g., "Best Picture"
    
    # Nominee information
    movie_title: Optional[str] = None
    movie_tmdb_id: Optional[int] = None
    movie_imdb_id: Optional[str] = None
    
    person_name: Optional[str] = None
    person_role: Optional[str] = None  # e.g., "Director", "Actor"
    
    # Status
    won: bool = False
    nominated: bool = True
    
    # Dates
    announcement_date: Optional[date] = None
    ceremony_date: Optional[date] = None
    
    # Additional info
    notes: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'award_name': self.award_name,
            'ceremony_year': self.ceremony_year,
            'ceremony_number': self.ceremony_number,
            'category': self.category,
            'movie_title': self.movie_title,
            'movie_tmdb_id': self.movie_tmdb_id,
            'movie_imdb_id': self.movie_imdb_id,
            'person_name': self.person_name,
            'person_role': self.person_role,
            'won': self.won,
            'nominated': self.nominated,
            'announcement_date': self.announcement_date.isoformat() if self.announcement_date else None,
            'ceremony_date': self.ceremony_date.isoformat() if self.ceremony_date else None,
            'notes': self.notes
        }


@dataclass
class BoxOfficeData:
    """Box office data model"""
    movie_tmdb_id: Optional[int] = None
    movie_title: str = ""
    date: Optional[date] = None
    daily_gross: Optional[int] = None
    total_gross: Optional[int] = None
    theater_count: Optional[int] = None
    rank: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'movie_tmdb_id': self.movie_tmdb_id,
            'movie_title': self.movie_title,
            'date': self.date.isoformat() if self.date else None,
            'daily_gross': self.daily_gross,
            'total_gross': self.total_gross,
            'theater_count': self.theater_count,
            'rank': self.rank
        }
