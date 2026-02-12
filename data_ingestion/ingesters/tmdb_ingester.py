"""
TMDb API Ingester
Fetches movie data from The Movie Database API
"""
import os
import requests
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime, date
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry

from models.data_models import Movie

logger = logging.getLogger(__name__)


class TMDbIngester:
    """Ingester for The Movie Database (TMDb) API"""
    
    BASE_URL = "https://api.themoviedb.org/3"
    RATE_LIMIT_CALLS = 40  # TMDb allows 40 requests per 10 seconds
    RATE_LIMIT_PERIOD = 10  # seconds
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TMDb ingester
        
        Args:
            api_key: TMDb API key (or set TMDB_API_KEY environment variable)
        """
        self.api_key = api_key or os.getenv('TMDB_API_KEY')
        if not self.api_key:
            raise ValueError("TMDb API key is required. Set TMDB_API_KEY environment variable.")
        
        self.session = requests.Session()
        logger.info("TMDb Ingester initialized")
    
    @sleep_and_retry
    @limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to TMDb API with rate limiting and retries
        
        Args:
            endpoint: API endpoint (e.g., '/movie/550')
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        if params is None:
            params = {}
        
        params['api_key'] = self.api_key
        
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_movie_details(self, tmdb_id: int) -> Movie:
        """
        Get comprehensive movie details
        
        Args:
            tmdb_id: TMDb movie ID
            
        Returns:
            Movie object with full details
        """
        logger.info(f"Fetching details for TMDb ID: {tmdb_id}")
        
        # Get movie details with credits, keywords, and external IDs
        data = self._make_request(
            f"/movie/{tmdb_id}",
            params={'append_to_response': 'credits,external_ids,keywords'}
        )
        
        # Parse movie data
        movie = Movie(
            tmdb_id=data.get('id'),
            imdb_id=data.get('external_ids', {}).get('imdb_id'),
            title=data.get('title', ''),
            original_title=data.get('original_title', ''),
            release_date=self._parse_date(data.get('release_date')),
            runtime_minutes=data.get('runtime'),
            budget=data.get('budget'),
            revenue=data.get('revenue'),
            original_language=data.get('original_language', ''),
            overview=data.get('overview', ''),
            tagline=data.get('tagline', ''),
            status=data.get('status', ''),
            genres=[g['name'] for g in data.get('genres', [])],
            production_countries=[c['name'] for c in data.get('production_countries', [])],
            production_companies=[c['name'] for c in data.get('production_companies', [])],
            spoken_languages=[l['english_name'] for l in data.get('spoken_languages', [])],
            tmdb_rating=data.get('vote_average'),
            tmdb_vote_count=data.get('vote_count'),
            poster_path=data.get('poster_path'),
            backdrop_path=data.get('backdrop_path'),
            homepage=data.get('homepage')
        )
        
        # Parse credits
        credits = data.get('credits', {})
        
        # Get director
        crew = credits.get('crew', [])
        directors = [c['name'] for c in crew if c['job'] == 'Director']
        if directors:
            movie.director = directors[0]
        
        # Get cast
        cast = credits.get('cast', [])
        movie.cast = [
            {
                'name': c['name'],
                'character': c['character'],
                'order': c['order']
            }
            for c in cast[:20]  # Top 20 cast members
        ]
        
        # Get key crew
        movie.crew = [
            {
                'name': c['name'],
                'job': c['job'],
                'department': c['department']
            }
            for c in crew
            if c['job'] in ['Director', 'Producer', 'Screenplay', 'Writer', 'Director of Photography']
        ]
        
        logger.info(f"Successfully fetched: {movie.title}")
        return movie
    
    def discover_movies(
        self,
        year: Optional[int] = None,
        min_vote_count: int = 100,
        sort_by: str = 'popularity.desc',
        max_pages: int = 10
    ) -> List[Movie]:
        """
        Discover movies using TMDb's discover endpoint
        
        Args:
            year: Release year to filter by
            min_vote_count: Minimum number of votes
            sort_by: Sort order (popularity.desc, vote_average.desc, etc.)
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List of Movie objects
        """
        logger.info(f"Discovering movies for year {year}")
        
        params = {
            'sort_by': sort_by,
            'vote_count.gte': min_vote_count,
            'page': 1
        }
        
        if year:
            params['primary_release_year'] = year
        
        movies = []
        
        for page in range(1, max_pages + 1):
            params['page'] = page
            
            data = self._make_request('/discover/movie', params=params)
            
            results = data.get('results', [])
            if not results:
                break
            
            for result in results:
                # Get full details for each movie
                try:
                    movie = self.get_movie_details(result['id'])
                    movies.append(movie)
                    time.sleep(0.25)  # Additional rate limiting
                except Exception as e:
                    logger.error(f"Error fetching movie {result.get('id')}: {e}")
                    continue
            
            logger.info(f"Processed page {page}/{data.get('total_pages', max_pages)}")
            
            if page >= data.get('total_pages', 0):
                break
        
        logger.info(f"Discovered {len(movies)} movies")
        return movies
    
    def search_movie(self, query: str, year: Optional[int] = None) -> List[Dict]:
        """
        Search for movies by title
        
        Args:
            query: Movie title to search for
            year: Optional year to filter results
            
        Returns:
            List of search results (simplified movie data)
        """
        logger.info(f"Searching for: {query}")
        
        params = {
            'query': query,
            'include_adult': False
        }
        
        if year:
            params['year'] = year
        
        data = self._make_request('/search/movie', params=params)
        results = data.get('results', [])
        
        logger.info(f"Found {len(results)} results for '{query}'")
        return results
    
    def get_popular_movies(self, page: int = 1) -> List[Dict]:
        """
        Get currently popular movies
        
        Args:
            page: Page number
            
        Returns:
            List of popular movies
        """
        data = self._make_request('/movie/popular', params={'page': page})
        return data.get('results', [])
    
    def get_top_rated_movies(self, page: int = 1) -> List[Dict]:
        """
        Get top rated movies
        
        Args:
            page: Page number
            
        Returns:
            List of top rated movies
        """
        data = self._make_request('/movie/top_rated', params={'page': page})
        return data.get('results', [])
    
    def get_movies_by_year_range(
        self,
        start_year: int,
        end_year: int,
        min_vote_count: int = 100
    ) -> List[Movie]:
        """
        Get movies across a range of years
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
            min_vote_count: Minimum vote count threshold
            
        Returns:
            List of Movie objects
        """
        all_movies = []
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Fetching movies for year: {year}")
            movies = self.discover_movies(
                year=year,
                min_vote_count=min_vote_count,
                max_pages=5  # Limit pages per year
            )
            all_movies.extend(movies)
            time.sleep(1)  # Be nice to the API
        
        logger.info(f"Total movies fetched: {len(all_movies)}")
        return all_movies
    
    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return None


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Initialize ingester
    ingester = TMDbIngester()
    
    # Example 1: Get specific movie
    movie = ingester.get_movie_details(tmdb_id=550)  # Fight Club
    print(f"\nMovie: {movie.title}")
    print(f"Director: {movie.director}")
    print(f"Release Date: {movie.release_date}")
    print(f"Rating: {movie.tmdb_rating}")
    
    # Example 2: Discover movies from 2023
    movies_2023 = ingester.discover_movies(year=2023, max_pages=2)
    print(f"\nFound {len(movies_2023)} movies from 2023")
    for movie in movies_2023[:5]:
        print(f"  - {movie.title} ({movie.release_date})")
    
    # Example 3: Search for a movie
    results = ingester.search_movie("Oppenheimer", year=2023)
    print(f"\nSearch results for 'Oppenheimer':")
    for result in results[:3]:
        print(f"  - {result['title']} (ID: {result['id']})")
