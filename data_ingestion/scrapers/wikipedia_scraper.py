"""
Improved Wikipedia Awards Scraper
More robust parsing with fallback methods
"""
import requests
import re
import logging
import time
from typing import List, Dict, Optional
from datetime import date, datetime
from bs4 import BeautifulSoup

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

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
    
    def _fetch_page(self, url: str, max_retries: int = 3) -> BeautifulSoup:
        """Fetch and parse Wikipedia page with retries"""
        logger.info(f"Fetching: {url}")
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return BeautifulSoup(response.content, 'html.parser')
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
    
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


class ImprovedAcademyAwardsScraper(WikipediaAwardsScraper):
    """
    Improved scraper for Academy Awards with multiple parsing strategies
    """
    
    # Major categories we care about
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
            
            # Try multiple parsing strategies
            awards = []
            
            # Strategy 1: Standard wikitable parsing
            awards = self._parse_standard_tables(soup, year, ceremony_number)
            
            # Strategy 2: If strategy 1 fails, try section-based parsing
            if not awards:
                logger.info("Standard parsing failed, trying section-based parsing")
                awards = self._parse_by_sections(soup, year, ceremony_number)
            
            # Strategy 3: Try finding lists in the content
            if not awards:
                logger.info("Section parsing failed, trying list-based parsing")
                awards = self._parse_lists(soup, year, ceremony_number)
            
            logger.info(f"Scraped {len(awards)} nominations/wins for year {year}")
            return awards
            
        except Exception as e:
            logger.error(f"Error scraping year {year}: {e}")
            logger.exception("Full traceback:")
            return []
    
    def _parse_standard_tables(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Standard wikitable-based parsing"""
        awards = []
        tables = soup.find_all('table', class_='wikitable')
        
        logger.info(f"Found {len(tables)} wikitables")
        
        for table in tables:
            category = self._find_category_header(table)
            
            if not category:
                continue
            
            if not self._is_major_category(category):
                logger.debug(f"Skipping category: {category}")
                continue
            
            logger.info(f"Parsing category: {category}")
            nominees = self._parse_nominees_table(table, category, year, ceremony_number)
            awards.extend(nominees)
        
        return awards
    
    def _parse_by_sections(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Parse by finding category sections (h2, h3 headers)"""
        awards = []
        
        # Find all section headers
        headers = soup.find_all(['h2', 'h3'])
        
        for header in headers:
            # Get section title
            headline = header.find('span', class_='mw-headline')
            if not headline:
                continue
            
            category = self._clean_text(headline.get_text())
            
            if not self._is_major_category(category):
                continue
            
            logger.info(f"Found section: {category}")
            
            # Find content after this header
            # Look for the next table, ul, or dl
            current = header.find_next_sibling()
            
            while current and current.name not in ['h2', 'h3', 'h4']:
                if current.name == 'table':
                    nominees = self._parse_nominees_table(
                        current, category, year, ceremony_number
                    )
                    awards.extend(nominees)
                    break
                elif current.name in ['ul', 'dl']:
                    nominees = self._parse_list_items(
                        current, category, year, ceremony_number
                    )
                    awards.extend(nominees)
                    break
                
                current = current.find_next_sibling()
        
        return awards
    
    def _parse_lists(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Parse from list elements (ul/dl)"""
        awards = []
        
        # Find all list elements
        lists = soup.find_all(['ul', 'dl'])
        
        for list_elem in lists:
            # Check if this list is under a relevant category
            prev_header = list_elem.find_previous(['h2', 'h3', 'h4'])
            
            if not prev_header:
                continue
            
            category = self._clean_text(prev_header.get_text())
            
            if not self._is_major_category(category):
                continue
            
            logger.info(f"Parsing list for category: {category}")
            nominees = self._parse_list_items(list_elem, category, year, ceremony_number)
            awards.extend(nominees)
        
        return awards
    
    def _parse_list_items(
        self,
        list_elem,
        category: str,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """Parse nominees from list items (li or dd elements)"""
        nominees = []
        items = list_elem.find_all(['li', 'dd'])
        
        for item in items:
            # Get text content
            text = self._clean_text(item.get_text())
            
            # Skip empty items
            if not text or len(text) < 3:
                continue
            
            # Check if winner (often has bold, or specific marker)
            is_winner = bool(item.find(['b', 'strong'])) or '(winner)' in text.lower()
            
            # Extract movie title and person
            links = item.find_all('a', href=True)
            
            if not links:
                continue
            
            movie_title = None
            person_name = None
            
            # First substantial link is usually the movie or person
            for link in links:
                link_text = self._clean_text(link.get_text())
                href = link.get('href', '')
                
                if not link_text or href.startswith('#'):
                    continue
                
                if movie_title is None:
                    movie_title = link_text
                elif person_name is None and self._is_person_category(category):
                    person_name = link_text
            
            # Determine person role
            person_role = None
            if person_name:
                person_role = self._get_person_role(category)
            
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
    
    def _find_category_header(self, table) -> Optional[str]:
        """Find the category header before a table"""
        # Look backwards for header tags
        current = table.find_previous(['h2', 'h3', 'h4'])
        
        if current:
            # Remove edit links
            for edit_link in current.find_all('span', class_='mw-editsection'):
                edit_link.decompose()
            
            # Try to get headline span
            headline = current.find('span', class_='mw-headline')
            if headline:
                category = self._clean_text(headline.get_text())
            else:
                category = self._clean_text(current.get_text())
            
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
            
            movie_title = None
            person_name = None
            
            # Parse links
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
            person_role = None
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
        # Method 1: Check for bold styling
        if row.find('b') or row.find('strong'):
            return True
        
        # Method 2: Check for background color
        style = row.get('style', '').lower()
        if 'background' in style:
            # Common winner background colors
            if any(color in style for color in ['#faeb86', '#ffc', 'gold', 'yellow', 'winner']):
                return True
        
        # Method 3: Check for trophy emoji or "Winner" text
        text = row.get_text()
        if '🏆' in text or 'winner' in text.lower():
            return True
        
        # Method 4: Check first cell for winner styling
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
                time.sleep(1)  # Be polite to Wikipedia
            except Exception as e:
                logger.error(f"Failed to scrape year {year}: {e}")
                continue
        
        logger.info(f"Total awards scraped: {len(all_awards)}")
        return all_awards


if __name__ == "__main__":
    # Test the improved scraper
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    scraper = ImprovedAcademyAwardsScraper()
    
    # Test with multiple years
    for test_year in [2023, 2024]:
        print(f"\n{'='*60}")
        print(f"Testing year: {test_year}")
        print('='*60)
        
        awards = scraper.scrape_year(test_year)
        print(f"\nTotal awards found: {len(awards)}")
        
        if awards:
            # Show Best Picture
            best_picture = [a for a in awards if 'Best Picture' in a.category]
            if best_picture:
                print(f"\nBest Picture nominees: {len(best_picture)}")
                for award in best_picture:
                    status = "🏆 WINNER" if award.won else "  Nominee"
                    print(f"{status}: {award.movie_title}")
            
            # Count categories
            from collections import Counter
            categories = Counter([a.category for a in awards])
            print(f"\nCategories found: {len(categories)}")
            for cat, count in categories.most_common(10):
                print(f"  {cat}: {count}")
        else:
            print("⚠ No awards found")