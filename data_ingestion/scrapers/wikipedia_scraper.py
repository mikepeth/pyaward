"""
Wikipedia Awards Scraper
Scrapes award data from Wikipedia
"""
import requests
import re
import logging
from typing import List, Dict, Optional
from datetime import date, datetime
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from models.data_models import Award

logger = logging.getLogger(__name__)


class WikipediaAwardsScraper:
    """Base scraper for Wikipedia awards pages"""
    
    BASE_URL = "https://en.wikipedia.org/wiki/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MovieAwardsResearchBot/1.0 (Educational/Research Purpose)'
        })
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch and parse Wikipedia page"""
        logger.info(f"Fetching: {url}")
        response = self.session.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean text by removing citations and extra whitespace"""
        # Remove citation brackets
        text = re.sub(r'\[\d+\]', '', text)
        # Remove edit links
        text = re.sub(r'\[edit\]', '', text)
        # Clean whitespace
        text = ' '.join(text.split())
        return text.strip()
    
    @staticmethod
    def _get_ordinal(n: int) -> str:
        """Convert number to ordinal string (1st, 2nd, 3rd, etc.)"""
        if 10 <= n % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
        return f"{n}{suffix}"


class AcademyAwardsScraper(WikipediaAwardsScraper):
    """Scraper for Academy Awards (Oscars) from Wikipedia"""
    
    # Major categories to scrape
    MAJOR_CATEGORIES = [
        'Best Picture',
        'Best Director',
        'Best Actor',
        'Best Actress',
        'Best Supporting Actor',
        'Best Supporting Actress',
        'Best Original Screenplay',
        'Best Adapted Screenplay',
        'Best Cinematography',
        'Best Film Editing',
        'Best Original Score',
        'Best Original Song',
        'Best Animated Feature',
        'Best International Feature Film',
        'Best Documentary Feature'
    ]
    
    def scrape_year(self, year: int) -> List[Award]:
        """
        Scrape Academy Awards for a specific ceremony year
        
        Args:
            year: Ceremony year (e.g., 2024 for 96th Academy Awards)
            
        Returns:
            List of Award objects
        """
        # Calculate ceremony number (first ceremony was 1929)
        ceremony_number = year - 1929 + 1
        ordinal = self._get_ordinal(ceremony_number)
        
        url = f"{self.BASE_URL}{ordinal}_Academy_Awards"
        logger.info(f"Scraping {ordinal} Academy Awards ({year})")
        
        try:
            soup = self._fetch_page(url)
            awards = self._parse_awards_page(soup, year, ceremony_number)
            logger.info(f"Scraped {len(awards)} nominations/wins for year {year}")
            return awards
        except Exception as e:
            logger.error(f"Error scraping year {year}: {e}")
            return []
    
    def _parse_awards_page(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Parse awards from the ceremony page"""
        awards = []
        
        # Method 1: Find tables with class 'wikitable'
        tables = soup.find_all('table', class_='wikitable')
        
        for table in tables:
            # Find the category header (usually in a preceding h3 or h4)
            category = self._find_category_header(table)
            
            if not category or not self._is_major_category(category):
                continue
            
            # Parse nominees from table
            nominees = self._parse_nominees_table(table, category, year, ceremony_number)
            awards.extend(nominees)
        
        # Method 2: Also check for structured data in infobox
        # Some years have different HTML structure
        if not awards:
            logger.warning(f"No awards found with standard method for {year}, trying alternative parsing")
            awards = self._parse_alternative_structure(soup, year, ceremony_number)
        
        return awards
    
    def _find_category_header(self, table) -> Optional[str]:
        """Find the category header before a table"""
        # Look backwards for header tags
        current = table.find_previous(['h2', 'h3', 'h4'])
        
        if current:
            # Remove edit links
            for edit_link in current.find_all('span', class_='mw-editsection'):
                edit_link.decompose()
            
            category = self._clean_text(current.get_text())
            
            # Sometimes the category is in a span with specific formatting
            if not category:
                span = current.find('span', class_='mw-headline')
                if span:
                    category = self._clean_text(span.get_text())
            
            return category
        
        return None
    
    def _is_major_category(self, category: str) -> bool:
        """Check if category is one we want to track"""
        category_lower = category.lower()
        for major_cat in self.MAJOR_CATEGORIES:
            if major_cat.lower() in category_lower:
                return True
        return False
    
    def _parse_nominees_table(
        self,
        table,
        category: str,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Parse nominees from a table"""
        nominees = []
        rows = table.find_all('tr')
        
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            
            if len(cells) < 1:
                continue
            
            # Check if this row represents a winner
            is_winner = self._is_winner_row(row)
            
            # Extract movie and person information
            first_cell = cells[0]
            
            # Get all links in the cell
            links = first_cell.find_all('a', href=True)
            
            if not links:
                continue
            
            # Usually: first link is the film, second link (if exists) is the person
            movie_title = None
            person_name = None
            person_role = None
            
            # Try to determine what the links represent
            for i, link in enumerate(links):
                link_text = self._clean_text(link.get_text())
                href = link.get('href', '')
                
                # Skip empty links or self-references
                if not link_text or href.startswith('#'):
                    continue
                
                # First substantial link is usually the movie
                if movie_title is None:
                    movie_title = link_text
                # Second link is often a person (for acting/directing categories)
                elif person_name is None and self._is_person_category(category):
                    person_name = link_text
            
            # Determine person role based on category
            if person_name:
                person_role = self._get_person_role(category)
            
            # Create award object
            award = Award(
                award_name="Academy Awards",
                ceremony_year=year,
                ceremony_number=ceremony_number,
                category=category,
                movie_title=movie_title,
                person_name=person_name,
                person_role=person_role,
                won=is_winner,
                nominated=True
            )
            
            nominees.append(award)
        
        return nominees
    
    def _is_winner_row(self, row) -> bool:
        """Determine if a row represents the winner"""
        # Check for bold styling
        if row.find('b') or row.find('strong'):
            return True
        
        # Check for background color (winners often have highlighted background)
        style = row.get('style', '').lower()
        if 'background' in style:
            # Common winner background colors
            if any(color in style for color in ['#faeb86', '#ffc', 'gold', 'yellow']):
                return True
        
        # Check for trophy emoji or "Winner" text
        text = row.get_text()
        if '🏆' in text or 'winner' in text.lower():
            return True
        
        # Check first cell for winner styling
        first_cell = row.find(['td', 'th'])
        if first_cell:
            cell_style = first_cell.get('style', '').lower()
            if 'background' in cell_style:
                return True
            
            # Check for winner class
            cell_class = ' '.join(first_cell.get('class', [])).lower()
            if 'winner' in cell_class:
                return True
        
        return False
    
    def _is_person_category(self, category: str) -> bool:
        """Check if category involves a person (actor, director, etc.)"""
        person_keywords = ['actor', 'actress', 'director', 'writer', 'screenplay']
        category_lower = category.lower()
        return any(keyword in category_lower for keyword in person_keywords)
    
    def _get_person_role(self, category: str) -> str:
        """Determine person role from category"""
        category_lower = category.lower()
        
        if 'director' in category_lower:
            return 'Director'
        elif 'actor' in category_lower or 'actress' in category_lower:
            if 'supporting' in category_lower:
                return 'Supporting Actor'
            return 'Lead Actor'
        elif 'screenplay' in category_lower or 'writer' in category_lower:
            return 'Writer'
        elif 'cinematography' in category_lower:
            return 'Cinematographer'
        elif 'editing' in category_lower:
            return 'Editor'
        elif 'score' in category_lower or 'song' in category_lower:
            return 'Composer'
        
        return 'Unknown'
    
    def _parse_alternative_structure(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Try alternative parsing methods for different page structures"""
        # This is a fallback for pages with different HTML structure
        # Implementation would depend on specific page variations
        logger.info("Using alternative parsing method")
        return []
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> List[Award]:
        """
        Scrape multiple years of Academy Awards
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
            
        Returns:
            List of all Award objects across years
        """
        all_awards = []
        
        for year in range(start_year, end_year + 1):
            try:
                awards = self.scrape_year(year)
                all_awards.extend(awards)
                logger.info(f"Year {year}: {len(awards)} nominations")
            except Exception as e:
                logger.error(f"Failed to scrape year {year}: {e}")
                continue
        
        logger.info(f"Total awards scraped: {len(all_awards)}")
        return all_awards


class GoldenGlobesScraper(WikipediaAwardsScraper):
    """Scraper for Golden Globe Awards from Wikipedia"""
    
    MAJOR_CATEGORIES = [
        'Best Motion Picture – Drama',
        'Best Motion Picture – Musical or Comedy',
        'Best Director',
        'Best Actor – Drama',
        'Best Actress – Drama',
        'Best Actor – Musical or Comedy',
        'Best Actress – Musical or Comedy',
        'Best Supporting Actor',
        'Best Supporting Actress',
        'Best Screenplay'
    ]
    
    def scrape_year(self, year: int) -> List[Award]:
        """
        Scrape Golden Globes for a specific year
        
        Args:
            year: Ceremony year
            
        Returns:
            List of Award objects
        """
        # Golden Globes URL structure
        ordinal = self._get_ordinal(year - 1944 + 1)  # First ceremony was 1944
        url = f"{self.BASE_URL}{ordinal}_Golden_Globe_Awards"
        
        logger.info(f"Scraping {ordinal} Golden Globe Awards ({year})")
        
        try:
            soup = self._fetch_page(url)
            # Similar parsing logic to Academy Awards
            # Implementation details would be similar
            awards = []  # Placeholder
            return awards
        except Exception as e:
            logger.error(f"Error scraping Golden Globes {year}: {e}")
            return []


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize scraper
    scraper = AcademyAwardsScraper()
    
    # Example 1: Scrape single year
    awards_2024 = scraper.scrape_year(2024)
    print(f"\n2024 Academy Awards: {len(awards_2024)} nominations")
    
    # Show Best Picture nominees
    best_picture = [a for a in awards_2024 if 'Best Picture' in a.category]
    print("\nBest Picture Nominees:")
    for award in best_picture:
        status = "🏆 WINNER" if award.won else "  Nominee"
        print(f"{status}: {award.movie_title}")
    
    # Example 2: Scrape multiple years
    print("\n" + "="*50)
    awards_range = scraper.scrape_multiple_years(2020, 2024)
    print(f"\nTotal awards 2020-2024: {len(awards_range)}")
    
    # Count by category
    from collections import Counter
    categories = Counter([a.category for a in awards_range])
    print("\nNominations by category:")
    for category, count in categories.most_common(5):
        print(f"  {category}: {count}")
