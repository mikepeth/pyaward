# Movie Awards Data Ingestion

Complete data ingestion system for collecting movie metadata and awards information from multiple sources.

## Features

- **TMDb API Integration**: Fetch comprehensive movie data including cast, crew, ratings, and box office
- **Wikipedia Scraping**: Extract Academy Awards (Oscars) nominations and winners
- **Data Matching**: Automatically match movies between different data sources
- **Multiple Formats**: Save data as JSON and CSV
- **Rate Limiting**: Built-in rate limiting to respect API limits
- **Retry Logic**: Automatic retries for failed requests
- **Logging**: Comprehensive logging for debugging and monitoring

## Quick Start

### 1. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env file from template
cp .env.template .env
```

### 2. Get API Keys

**TMDb API Key** (Required):
1. Go to https://www.themoviedb.org/settings/api
2. Sign up for a free account
3. Request an API key
4. Add to `.env` file: `TMDB_API_KEY=your_key_here`

**OMDb API Key** (Optional):
1. Go to http://www.omdbapi.com/apikey.aspx
2. Get a free API key
3. Add to `.env` file: `OMDB_API_KEY=your_key_here`

### 3. Run Data Collection

```bash
# Collect movies from a single year
python collect_data.py --mode movies --year 2023

# Collect Academy Awards from a single year
python collect_data.py --mode awards --year 2024

# Collect everything from 2010-2024
python collect_data.py --mode full --start-year 2010 --end-year 2024
```

## Usage Examples

### Example 1: Collect Movies from 2023

```python
from ingesters.tmdb_ingester import TMDbIngester
from utils.storage import DataStorage

# Initialize
ingester = TMDbIngester()
storage = DataStorage('./data')

# Fetch movies
movies = ingester.discover_movies(year=2023, max_pages=5)

# Save data
storage.save_movies_to_json(movies, 'movies_2023.json')
storage.save_movies_to_csv(movies, 'movies_2023.csv')

print(f"Collected {len(movies)} movies from 2023")
```

### Example 2: Scrape Academy Awards

```python
from scrapers.wikipedia_scraper import AcademyAwardsScraper
from utils.storage import DataStorage

# Initialize
scraper = AcademyAwardsScraper()
storage = DataStorage('./data')

# Scrape 2024 Academy Awards
awards = scraper.scrape_year(2024)

# Save data
storage.save_awards_to_json(awards, 'oscars_2024.json')

# Show Best Picture nominees
best_picture = [a for a in awards if 'Best Picture' in a.category]
for award in best_picture:
    status = "WINNER" if award.won else "Nominee"
    print(f"{status}: {award.movie_title}")
```

### Example 3: Get Specific Movie Details

```python
from ingesters.tmdb_ingester import TMDbIngester

ingester = TMDbIngester()

# Get movie by TMDb ID
movie = ingester.get_movie_details(tmdb_id=550)  # Fight Club

print(f"Title: {movie.title}")
print(f"Director: {movie.director}")
print(f"Rating: {movie.tmdb_rating}")
print(f"Budget: ${movie.budget:,}")
print(f"Revenue: ${movie.revenue:,}")
```

### Example 4: Search for Movies

```python
from ingesters.tmdb_ingester import TMDbIngester

ingester = TMDbIngester()

# Search for a movie
results = ingester.search_movie("Oppenheimer", year=2023)

for movie in results:
    print(f"{movie['title']} (ID: {movie['id']})")
```

### Example 5: Historical Data Collection

```python
from collect_data import DataCollectionPipeline

pipeline = DataCollectionPipeline()

# Collect all data from 2020-2024
movies, awards = pipeline.full_historical_collection(
    start_year=2020,
    end_year=2024
)

print(f"Collected {len(movies)} movies")
print(f"Collected {len(awards)} award nominations")
```

## Command Line Interface

### Collect Movies

```bash
# Single year
python collect_data.py --mode movies --year 2023

# Year range
python collect_data.py --mode movies --start-year 2020 --end-year 2024

# Custom data directory
python collect_data.py --mode movies --year 2023 --data-dir ./my_data
```

### Collect Awards

```bash
# Single year
python collect_data.py --mode awards --year 2024

# Year range
python collect_data.py --mode awards --start-year 2020 --end-year 2024
```

### Full Collection

```bash
# Collect both movies and awards
python collect_data.py --mode full --start-year 2015 --end-year 2024
```

## Data Structure

### Movie Data

```json
{
  "tmdb_id": 550,
  "imdb_id": "tt0137523",
  "title": "Fight Club",
  "original_title": "Fight Club",
  "release_date": "1999-10-15",
  "runtime_minutes": 139,
  "budget": 63000000,
  "revenue": 100853753,
  "original_language": "en",
  "genres": ["Drama"],
  "production_countries": ["United States of America"],
  "tmdb_rating": 8.4,
  "tmdb_vote_count": 26280,
  "director": "David Fincher",
  "cast": [
    {
      "name": "Brad Pitt",
      "character": "Tyler Durden",
      "order": 0
    }
  ]
}
```

### Award Data

```json
{
  "award_name": "Academy Awards",
  "ceremony_year": 2024,
  "ceremony_number": 96,
  "category": "Best Picture",
  "movie_title": "Oppenheimer",
  "won": true,
  "nominated": true
}
```

## Project Structure

```
data_ingestion/
├── collect_data.py          # Main orchestration script
├── requirements.txt         # Python dependencies
├── .env.template           # Environment variables template
│
├── ingesters/
│   └── tmdb_ingester.py    # TMDb API client
│
├── scrapers/
│   └── wikipedia_scraper.py # Wikipedia awards scraper
│
├── models/
│   └── data_models.py      # Data models (Movie, Award, etc.)
│
├── utils/
│   └── storage.py          # Data storage utilities
│
└── data/
    ├── raw/                # Raw data files
    └── processed/          # Processed data files
```

## Available Data Sources

### TMDb (The Movie Database)
- Movie metadata (title, release date, runtime, budget, revenue)
- Cast and crew information
- Genres and production companies
- User ratings and vote counts
- IMDb IDs for cross-referencing

### Wikipedia
- Academy Awards (Oscars) nominations and winners
- Golden Globe Awards (structure ready, needs implementation)
- Major categories including Best Picture, Acting, Directing, Writing

## Data Output

All data is saved in two formats:

1. **JSON**: Complete structured data with nested objects
2. **CSV**: Flattened data for easy analysis in spreadsheets

Files are saved with timestamps:
- `movies_2023_20240115.json`
- `academy_awards_2024_20240115.csv`

## Rate Limiting

The system respects API rate limits:

- **TMDb**: 40 requests per 10 seconds
- **Wikipedia**: Polite crawling with delays between requests

## Error Handling

- Automatic retries with exponential backoff
- Comprehensive error logging
- Graceful handling of missing data
- Continue processing even if individual requests fail

## Advanced Usage

### Custom Movie Filtering

```python
from ingesters.tmdb_ingester import TMDbIngester

ingester = TMDbIngester()

# Get only highly-rated movies
movies = ingester.discover_movies(
    year=2023,
    min_vote_count=500,  # More restrictive
    sort_by='vote_average.desc'  # Sort by rating
)
```

### Scrape Multiple Years of Awards

```python
from scrapers.wikipedia_scraper import AcademyAwardsScraper

scraper = AcademyAwardsScraper()

# Scrape 10 years of data
awards = scraper.scrape_multiple_years(
    start_year=2015,
    end_year=2024
)

# Analyze by category
from collections import Counter
categories = Counter([a.category for a in awards])
print(categories.most_common(10))
```

### Match Movies to Awards

```python
from utils.storage import DataStorage, DataMatcher

storage = DataStorage()

# Load data
movies = storage.load_movies_from_json('movies_2023.json')
awards = storage.load_awards_from_json('academy_awards_2024.json')

# Enrich awards with TMDb IDs
enriched = DataMatcher.enrich_awards_with_movie_data(awards, movies)

# Now awards have tmdb_id and imdb_id fields
```

## Troubleshooting

### "TMDb API key is required"
Make sure you've set `TMDB_API_KEY` in your `.env` file.

### "Rate limit exceeded"
The code includes automatic rate limiting. If you still hit limits, increase the delay between requests.

### "No awards found"
Wikipedia page structure may have changed. Check the logs for parsing errors.

### Import Errors
Make sure you're running from the `data_ingestion` directory:
```bash
cd data_ingestion
python collect_data.py --mode movies --year 2023
```

## Next Steps

After collecting data:

1. **Data Cleaning**: Remove duplicates, handle missing values
2. **Data Enrichment**: Add box office data, critic scores
3. **Feature Engineering**: Create features for ML models
4. **Database Import**: Load data into PostgreSQL
5. **Model Training**: Build prediction models

## Contributing

To add new data sources:

1. Create a new ingester in `ingesters/` or scraper in `scrapers/`
2. Implement the standard interface (similar to existing ones)
3. Add to `collect_data.py` orchestration
4. Update this README

## License

This is a research/educational project. Please respect:
- TMDb API terms of service
- Wikipedia's robots.txt and crawling policies
- Copyright and licensing of scraped data

## Support

For issues or questions:
1. Check the logs in `data_ingestion.log`
2. Review the example code above
3. Check API documentation:
   - TMDb: https://developers.themoviedb.org/3
   - Wikipedia API: https://www.mediawiki.org/wiki/API

## Changelog

### Version 1.0.0 (Current)
- TMDb movie data collection
- Academy Awards scraping from Wikipedia
- JSON and CSV export
- Rate limiting and retry logic
- Data matching utilities
