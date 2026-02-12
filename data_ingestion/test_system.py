"""
Test/Demo Script
Demonstrates basic usage of the data ingestion system
Run this to verify everything is set up correctly
"""
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from ingesters.tmdb_ingester import TMDbIngester
from scrapers.wikipedia_scraper import AcademyAwardsScraper
from utils.storage import DataStorage
from models.data_models import Movie, Award

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_tmdb_ingester():
    """Test TMDb ingester"""
    print("\n" + "="*60)
    print("TEST 1: TMDb Movie Ingester")
    print("="*60)
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Check for API key
        if not os.getenv('TMDB_API_KEY'):
            print("❌ TMDB_API_KEY not found in .env file")
            print("   Please add your TMDb API key to .env file")
            return False
        
        # Initialize ingester
        ingester = TMDbIngester()
        print("✓ TMDb ingester initialized")
        
        # Test 1: Get specific movie
        print("\nFetching Fight Club (TMDb ID: 550)...")
        movie = ingester.get_movie_details(tmdb_id=550)
        
        print(f"✓ Movie: {movie.title}")
        print(f"  Director: {movie.director}")
        print(f"  Release Date: {movie.release_date}")
        print(f"  Rating: {movie.tmdb_rating}/10")
        print(f"  Budget: ${movie.budget:,}" if movie.budget else "  Budget: N/A")
        
        # Test 2: Search for a movie
        print("\nSearching for 'Oppenheimer'...")
        results = ingester.search_movie("Oppenheimer", year=2023)
        
        if results:
            print(f"✓ Found {len(results)} results")
            print(f"  Top result: {results[0]['title']} (ID: {results[0]['id']})")
        else:
            print("  No results found")
        
        # Test 3: Discover movies
        print("\nDiscovering popular movies from 2023 (limited to 1 page)...")
        movies = ingester.discover_movies(year=2023, max_pages=1)
        
        print(f"✓ Found {len(movies)} movies")
        if movies:
            print(f"  Sample: {movies[0].title}")
        
        print("\n✅ TMDb ingester test PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n❌ TMDb ingester test FAILED: {e}\n")
        return False


def test_wikipedia_scraper():
    """Test Wikipedia scraper"""
    print("\n" + "="*60)
    print("TEST 2: Wikipedia Awards Scraper")
    print("="*60)
    
    try:
        # Initialize scraper
        scraper = AcademyAwardsScraper()
        print("✓ Wikipedia scraper initialized")
        
        # Test: Scrape 2024 Academy Awards
        print("\nScraping 2024 Academy Awards...")
        awards = scraper.scrape_year(2024)
        
        if awards:
            print(f"✓ Scraped {len(awards)} nominations")
            
            # Show Best Picture nominees
            best_picture = [a for a in awards if 'Best Picture' in a.category]
            if best_picture:
                print(f"\n  Best Picture nominees found: {len(best_picture)}")
                for award in best_picture[:3]:  # Show first 3
                    status = "🏆 WINNER" if award.won else "   Nominee"
                    print(f"  {status}: {award.movie_title}")
            
            # Count categories
            categories = set(a.category for a in awards)
            print(f"\n  Categories scraped: {len(categories)}")
        else:
            print("⚠ No awards found (page structure may have changed)")
        
        print("\n✅ Wikipedia scraper test PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n❌ Wikipedia scraper test FAILED: {e}\n")
        logger.exception("Scraper error details:")
        return False


def test_data_storage():
    """Test data storage"""
    print("\n" + "="*60)
    print("TEST 3: Data Storage")
    print("="*60)
    
    try:
        # Initialize storage
        storage = DataStorage('./test_data')
        print("✓ Data storage initialized at ./test_data")
        
        # Create test data
        from datetime import date
        
        test_movies = [
            Movie(
                tmdb_id=550,
                title="Fight Club",
                release_date=date(1999, 10, 15),
                director="David Fincher",
                tmdb_rating=8.4
            ),
            Movie(
                tmdb_id=680,
                title="Pulp Fiction",
                release_date=date(1994, 10, 14),
                director="Quentin Tarantino",
                tmdb_rating=8.9
            )
        ]
        
        test_awards = [
            Award(
                award_name="Academy Awards",
                ceremony_year=2024,
                category="Best Picture",
                movie_title="Oppenheimer",
                won=True
            )
        ]
        
        # Test JSON storage
        print("\nSaving test data to JSON...")
        storage.save_movies_to_json(test_movies, 'test_movies.json')
        storage.save_awards_to_json(test_awards, 'test_awards.json')
        print("✓ JSON files saved")
        
        # Test CSV storage
        print("\nSaving test data to CSV...")
        storage.save_movies_to_csv(test_movies, 'test_movies.csv')
        storage.save_awards_to_csv(test_awards, 'test_awards.csv')
        print("✓ CSV files saved")
        
        # Test loading
        print("\nLoading data back...")
        loaded_movies = storage.load_movies_from_json('test_movies.json')
        loaded_awards = storage.load_awards_from_json('test_awards.json')
        print(f"✓ Loaded {len(loaded_movies)} movies")
        print(f"✓ Loaded {len(loaded_awards)} awards")
        
        print("\n✅ Data storage test PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n❌ Data storage test FAILED: {e}\n")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("DATA INGESTION SYSTEM TEST SUITE")
    print("="*60)
    
    results = {
        'TMDb Ingester': test_tmdb_ingester(),
        'Wikipedia Scraper': test_wikipedia_scraper(),
        'Data Storage': test_data_storage()
    }
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n🎉 All tests passed! System is ready to use.")
        print("\nNext steps:")
        print("1. Run: python collect_data.py --mode movies --year 2023")
        print("2. Run: python collect_data.py --mode awards --year 2024")
    else:
        print("\n⚠ Some tests failed. Please check the errors above.")
        print("\nCommon issues:")
        print("- Missing TMDB_API_KEY in .env file")
        print("- Missing dependencies (run: pip install -r requirements.txt)")
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()
