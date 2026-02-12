"""
Main Data Collection Script
Orchestrates data ingestion from multiple sources
"""
import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from ingesters.tmdb_ingester import TMDbIngester
from scrapers.wikipedia_scraper import ImprovedAcademyAwardsScraper as AcademyAwardsScraper
from utils.storage import DataStorage, DataMatcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCollectionPipeline:
    """Main pipeline for collecting movie and awards data"""
    
    def __init__(self, data_dir: str = './data'):
        """
        Initialize data collection pipeline
        
        Args:
            data_dir: Directory for data storage
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize components
        self.tmdb_ingester = TMDbIngester()
        self.oscars_scraper = AcademyAwardsScraper()
        self.storage = DataStorage(data_dir)
        
        logger.info("Data Collection Pipeline initialized")
    
    def collect_movies_for_year(self, year: int, max_pages: int = 10):
        """
        Collect movies released in a specific year
        
        Args:
            year: Year to collect movies for
            max_pages: Maximum pages to fetch from TMDb
        """
        logger.info(f"Collecting movies for year {year}")
        
        # Fetch movies from TMDb
        movies = self.tmdb_ingester.discover_movies(
            year=year,
            min_vote_count=50,  # Lower threshold to get more movies
            max_pages=max_pages
        )
        
        # Save movies
        filename = f'movies_{year}_{datetime.now().strftime("%Y%m%d")}.json'
        self.storage.save_movies_to_json(movies, filename)
        
        # Also save as CSV
        csv_filename = f'movies_{year}_{datetime.now().strftime("%Y%m%d")}.csv'
        self.storage.save_movies_to_csv(movies, csv_filename)
        
        logger.info(f"Collected {len(movies)} movies for year {year}")
        return movies
    
    def collect_movies_for_year_range(
        self,
        start_year: int,
        end_year: int,
        max_pages_per_year: int = 5
    ):
        """
        Collect movies for a range of years
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
            max_pages_per_year: Max pages per year
        """
        logger.info(f"Collecting movies from {start_year} to {end_year}")
        
        all_movies = []
        
        for year in range(start_year, end_year + 1):
            try:
                movies = self.collect_movies_for_year(year, max_pages_per_year)
                all_movies.extend(movies)
            except Exception as e:
                logger.error(f"Error collecting movies for {year}: {e}")
                continue
        
        # Save combined dataset
        filename = f'movies_{start_year}_{end_year}_{datetime.now().strftime("%Y%m%d")}.json'
        self.storage.save_movies_to_json(all_movies, filename)
        
        logger.info(f"Total movies collected: {len(all_movies)}")
        return all_movies
    
    def collect_academy_awards_for_year(self, year: int):
        """
        Collect Academy Awards for a specific year
        
        Args:
            year: Ceremony year
        """
        logger.info(f"Collecting Academy Awards for {year}")
        
        # Scrape awards
        awards = self.oscars_scraper.scrape_year(year)
        
        # Save awards
        filename = f'academy_awards_{year}_{datetime.now().strftime("%Y%m%d")}.json'
        self.storage.save_awards_to_json(awards, filename)
        
        # Also save as CSV
        csv_filename = f'academy_awards_{year}_{datetime.now().strftime("%Y%m%d")}.csv'
        self.storage.save_awards_to_csv(awards, csv_filename)
        
        logger.info(f"Collected {len(awards)} Academy Awards nominations for {year}")
        return awards
    
    def collect_academy_awards_for_year_range(
        self,
        start_year: int,
        end_year: int
    ):
        """
        Collect Academy Awards for a range of years
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
        """
        logger.info(f"Collecting Academy Awards from {start_year} to {end_year}")
        
        all_awards = self.oscars_scraper.scrape_multiple_years(start_year, end_year)
        
        # Save combined dataset
        filename = f'academy_awards_{start_year}_{end_year}_{datetime.now().strftime("%Y%m%d")}.json'
        self.storage.save_awards_to_json(all_awards, filename)
        
        # Also save as CSV
        csv_filename = f'academy_awards_{start_year}_{end_year}_{datetime.now().strftime("%Y%m%d")}.csv'
        self.storage.save_awards_to_csv(all_awards, csv_filename)
        
        logger.info(f"Total Academy Awards collected: {len(all_awards)}")
        return all_awards
    
    def enrich_awards_with_movie_data(
        self,
        awards_filename: str,
        movies_filename: str,
        output_filename: str = None
    ):
        """
        Enrich awards data with movie metadata
        
        Args:
            awards_filename: Awards JSON file
            movies_filename: Movies JSON file
            output_filename: Output filename (auto-generated if None)
        """
        logger.info("Enriching awards with movie data")
        
        # Load data
        awards = self.storage.load_awards_from_json(awards_filename)
        movies = self.storage.load_movies_from_json(movies_filename)
        
        # Enrich
        enriched_awards = DataMatcher.enrich_awards_with_movie_data(awards, movies)
        
        # Save
        if output_filename is None:
            output_filename = f'enriched_awards_{datetime.now().strftime("%Y%m%d")}.json'
        
        with open(self.storage.processed_dir / output_filename, 'w') as f:
            import json
            json.dump(enriched_awards, f, indent=2)
        
        logger.info(f"Enriched {len(enriched_awards)} awards")
        return enriched_awards
    
    def full_historical_collection(
        self,
        start_year: int = 2010,
        end_year: int = 2024
    ):
        """
        Perform full historical data collection
        
        Args:
            start_year: Starting year
            end_year: Ending year
        """
        logger.info(f"Starting full historical collection: {start_year}-{end_year}")
        
        # Collect movies
        logger.info("Step 1/2: Collecting movie data")
        movies = self.collect_movies_for_year_range(start_year, end_year)
        
        # Collect awards
        logger.info("Step 2/2: Collecting awards data")
        awards = self.collect_academy_awards_for_year_range(start_year, end_year)
        
        logger.info("Historical collection complete!")
        logger.info(f"  Movies: {len(movies)}")
        logger.info(f"  Awards: {len(awards)}")
        
        return movies, awards


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Movie Awards Data Collection')
    
    parser.add_argument(
        '--mode',
        choices=['movies', 'awards', 'full', 'enrich'],
        required=True,
        help='Collection mode'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        help='Single year to collect'
    )
    
    parser.add_argument(
        '--start-year',
        type=int,
        default=2010,
        help='Start year for range collection'
    )
    
    parser.add_argument(
        '--end-year',
        type=int,
        default=2024,
        help='End year for range collection'
    )
    
    parser.add_argument(
        '--data-dir',
        default='./data',
        help='Data directory'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = DataCollectionPipeline(data_dir=args.data_dir)
    
    # Execute based on mode
    if args.mode == 'movies':
        if args.year:
            pipeline.collect_movies_for_year(args.year)
        else:
            pipeline.collect_movies_for_year_range(args.start_year, args.end_year)
    
    elif args.mode == 'awards':
        if args.year:
            pipeline.collect_academy_awards_for_year(args.year)
        else:
            pipeline.collect_academy_awards_for_year_range(args.start_year, args.end_year)
    
    elif args.mode == 'full':
        pipeline.full_historical_collection(args.start_year, args.end_year)
    
    elif args.mode == 'enrich':
        # Example enrichment
        print("Enrich mode: Please modify the script to specify input files")
    
    logger.info("Data collection completed successfully!")


if __name__ == "__main__":
    main()
