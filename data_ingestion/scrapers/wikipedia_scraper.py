"""
Wikipedia Awards Scraper
Scrapes award nominations and wins from Wikipedia for multiple award ceremonies
and film festivals. Focused on acting and screenplay categories.
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
                time.sleep(2 ** attempt)

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean text by removing citations and extra whitespace"""
        text = re.sub(r'\[\d+\]', '', text)
        text = re.sub(r'\[edit\]', '', text)
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


class BaseCeremonyAwardsScraper(WikipediaAwardsScraper):
    """
    Base class for ceremony-style award scrapers (Oscar, BAFTA, Globes, etc.).
    Subclasses define AWARD_NAME, MAJOR_CATEGORIES, and implement scrape_year().
    """

    AWARD_NAME = ""
    MAJOR_CATEGORIES: List[str] = []

    # ------------------------------------------------------------------ #
    # Public interface                                                     #
    # ------------------------------------------------------------------ #

    def scrape_year(self, year: int) -> List[Award]:
        raise NotImplementedError

    def scrape_multiple_years(self, start_year: int, end_year: int) -> List[Award]:
        all_awards = []
        for year in range(start_year, end_year + 1):
            try:
                awards = self.scrape_year(year)
                all_awards.extend(awards)
                logger.info(f"{self.AWARD_NAME} year {year}: {len(awards)} nominations")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Failed to scrape {self.AWARD_NAME} year {year}: {e}")
                continue
        logger.info(f"Total {self.AWARD_NAME} awards scraped: {len(all_awards)}")
        return all_awards

    # ------------------------------------------------------------------ #
    # Shared parsing helpers                                               #
    # ------------------------------------------------------------------ #

    def _scrape_page(
        self,
        url: str,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        """
        Fetch page and try multiple parsing strategies in order:
        1. Standard wikitable rows with preceding H2/H3 header
        2. Colored-div category headers + nested <ul>/<li> nominees
           (used by Academy Awards, BAFTAs, Globes, SAG, Critics' Choice, etc.)
        3. <th> column headers + <td> nominees side-by-side
           (used by Independent Spirit Awards)
        4. Section-based (H2/H3 headers → nearby tables or lists)
        5. List-based fallback
        """
        soup = self._fetch_page(url)

        awards = self._parse_standard_tables(soup, year, ceremony_number)
        if not awards:
            awards = self._parse_presentation_table(soup, year, ceremony_number)
        if not awards:
            awards = self._parse_th_header_tables(soup, year, ceremony_number)
        if not awards:
            logger.info("Trying section-based parsing")
            awards = self._parse_by_sections(soup, year, ceremony_number)
        if not awards:
            logger.info("Trying list-based parsing")
            awards = self._parse_lists(soup, year, ceremony_number)

        logger.info(f"Scraped {len(awards)} nominations/wins for {self.AWARD_NAME} {year}")
        return awards

    def _parse_presentation_table(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int,
    ) -> List[Award]:
        """
        Parse the colored-div + nested-list format used by most modern award
        Wikipedia pages (Academy Awards, BAFTAs, Golden Globes, SAG, Critics'
        Choice, etc.).

        Each <td> contains:
          <div style="...background-color:#XXXXXX..."><b><a>CATEGORY</a></b></div>
          <ul>
            <li><b>WINNER links</b>
              <ul>
                <li>Nominee links</li> ...
              </ul>
            </li>
          </ul>
        """
        awards = []

        for td in soup.find_all('td'):
            cat_div = td.find(
                'div',
                style=lambda s: s and re.search(r'background-color\s*:\s*#[0-9A-Fa-f]{3,6}', s)
            )
            if not cat_div:
                continue

            category = self._clean_text(cat_div.get_text())
            if not self._is_major_category(category):
                continue

            ul = td.find('ul')
            if not ul:
                continue

            awards.extend(
                self._parse_winner_nominee_list(ul, category, year, ceremony_number)
            )

        return awards

    def _parse_th_header_tables(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int,
    ) -> List[Award]:
        """
        Parse the <th> column-header layout used by the Independent Spirit Awards.

        Each table has alternating rows:
          <tr><th>Category A</th><th>Category B</th></tr>
          <tr><td>nominees for A</td><td>nominees for B</td></tr>
        """
        awards = []

        for table in soup.find_all('table', class_='wikitable'):
            rows = table.find_all('tr')
            for i, row in enumerate(rows):
                th_cells = row.find_all('th')
                if not th_cells:
                    continue

                # Collect major categories from this header row
                categories = []
                for th in th_cells:
                    cat = self._clean_text(th.get_text())
                    categories.append(cat if self._is_major_category(cat) else None)

                if not any(categories):
                    continue

                # Look for the next data row
                if i + 1 >= len(rows):
                    continue
                data_row = rows[i + 1]
                td_cells = data_row.find_all('td')

                for col_idx, (cat, td) in enumerate(zip(categories, td_cells)):
                    if not cat or col_idx >= len(td_cells):
                        continue

                    # Winner in <p><b>...</b></p>, nominees in <ul><li>
                    is_person_cat = self._is_person_category(cat)

                    winner_p = td.find('p')
                    if winner_p and winner_p.find('b'):
                        w_links = winner_p.find_all('a', href=True)
                        w_movie, w_person = self._extract_movie_and_person(
                            w_links, cat, person_first=is_person_cat
                        )
                        if w_movie or w_person:
                            awards.append(Award(
                                award_name=self.AWARD_NAME,
                                ceremony_year=year,
                                ceremony_number=ceremony_number,
                                category=cat,
                                movie_title=w_movie,
                                person_name=w_person,
                                person_role=self._get_person_role(cat) if w_person else None,
                                won=True,
                                nominated=True,
                            ))

                    ul = td.find('ul')
                    if ul:
                        for li in ul.find_all('li', recursive=False):
                            n_links = li.find_all('a', href=True)
                            n_movie, n_person = self._extract_movie_and_person(
                                n_links, cat, person_first=is_person_cat
                            )
                            if n_movie or n_person:
                                awards.append(Award(
                                    award_name=self.AWARD_NAME,
                                    ceremony_year=year,
                                    ceremony_number=ceremony_number,
                                    category=cat,
                                    movie_title=n_movie,
                                    person_name=n_person,
                                    person_role=self._get_person_role(cat) if n_person else None,
                                    won=False,
                                    nominated=True,
                                ))

        return awards

    def _parse_winner_nominee_list(
        self,
        ul,
        category: str,
        year: int,
        ceremony_number: int,
    ) -> List[Award]:
        """
        Parse a <ul> whose top-level <li> is the winner (bolded) and whose
        nested <ul><li> elements are the other nominees.
        """
        awards = []
        is_person_cat = self._is_person_category(category)

        for top_li in ul.find_all('li', recursive=False):
            is_winner = bool(top_li.find('b'))
            nested_ul = top_li.find('ul')

            # Collect links from this li BEFORE the nested ul
            top_links = []
            for child in top_li.children:
                if hasattr(child, 'name') and child.name == 'ul':
                    break
                if hasattr(child, 'find_all'):
                    top_links.extend(child.find_all('a', href=True))
                elif hasattr(child, 'name') and child.name == 'a':
                    top_links.append(child)

            movie_title, person_name = self._extract_movie_and_person(
                top_links, category, person_first=is_person_cat
            )
            if movie_title or person_name:
                awards.append(Award(
                    award_name=self.AWARD_NAME,
                    ceremony_year=year,
                    ceremony_number=ceremony_number,
                    category=category,
                    movie_title=movie_title,
                    person_name=person_name,
                    person_role=self._get_person_role(category) if person_name else None,
                    won=is_winner,
                    nominated=True,
                ))

            if nested_ul:
                for nested_li in nested_ul.find_all('li', recursive=False):
                    n_links = nested_li.find_all('a', href=True)
                    n_movie, n_person = self._extract_movie_and_person(
                        n_links, category, person_first=is_person_cat
                    )
                    if n_movie or n_person:
                        awards.append(Award(
                            award_name=self.AWARD_NAME,
                            ceremony_year=year,
                            ceremony_number=ceremony_number,
                            category=category,
                            movie_title=n_movie,
                            person_name=n_person,
                            person_role=self._get_person_role(category) if n_person else None,
                            won=False,
                            nominated=True,
                        ))

        return awards

    def _parse_standard_tables(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        awards = []
        tables = soup.find_all('table', class_='wikitable')
        logger.info(f"Found {len(tables)} wikitables")

        for table in tables:
            category = self._find_category_header(table)
            if not category or not self._is_major_category(category):
                continue
            logger.info(f"Parsing category: {category}")
            awards.extend(self._parse_nominees_table(table, category, year, ceremony_number))

        return awards

    def _parse_by_sections(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        awards = []
        for header in soup.find_all(['h2', 'h3']):
            headline = header.find('span', class_='mw-headline')
            if not headline:
                continue
            category = self._clean_text(headline.get_text())
            if not self._is_major_category(category):
                continue
            logger.info(f"Found section: {category}")
            current = header.find_next_sibling()
            while current and current.name not in ['h2', 'h3', 'h4']:
                if current.name == 'table':
                    awards.extend(
                        self._parse_nominees_table(current, category, year, ceremony_number)
                    )
                    break
                elif current.name in ['ul', 'dl']:
                    awards.extend(
                        self._parse_list_items(current, category, year, ceremony_number)
                    )
                    break
                current = current.find_next_sibling()
        return awards

    def _parse_lists(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        awards = []
        for list_elem in soup.find_all(['ul', 'dl']):
            prev_header = list_elem.find_previous(['h2', 'h3', 'h4'])
            if not prev_header:
                continue
            category = self._clean_text(prev_header.get_text())
            if not self._is_major_category(category):
                continue
            logger.info(f"Parsing list for category: {category}")
            awards.extend(self._parse_list_items(list_elem, category, year, ceremony_number))
        return awards

    def _parse_list_items(
        self,
        list_elem,
        category: str,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        nominees = []
        for item in list_elem.find_all(['li', 'dd']):
            text = self._clean_text(item.get_text())
            if not text or len(text) < 3:
                continue

            is_winner = bool(item.find(['b', 'strong'])) or '(winner)' in text.lower()
            links = item.find_all('a', href=True)
            if not links:
                continue

            movie_title, person_name = self._extract_movie_and_person(links, category)
            nominees.append(Award(
                award_name=self.AWARD_NAME,
                ceremony_year=year,
                ceremony_number=ceremony_number,
                category=category,
                movie_title=movie_title,
                person_name=person_name,
                person_role=self._get_person_role(category) if person_name else None,
                won=is_winner,
                nominated=True
            ))
        return nominees

    def _parse_nominees_table(
        self,
        table,
        category: str,
        year: int,
        ceremony_number: int
    ) -> List[Award]:
        nominees = []
        rows = table.find_all('tr')
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
            is_winner = self._is_winner_row(row)
            links = cells[0].find_all('a', href=True)
            if not links:
                continue
            movie_title, person_name = self._extract_movie_and_person(links, category)
            nominees.append(Award(
                award_name=self.AWARD_NAME,
                ceremony_year=year,
                ceremony_number=ceremony_number,
                category=category,
                movie_title=movie_title,
                person_name=person_name,
                person_role=self._get_person_role(category) if person_name else None,
                won=is_winner,
                nominated=True
            ))
        return nominees

    def _extract_movie_and_person(self, links, category: str, person_first: bool = False):
        """
        Return (movie_title, person_name) from a list of anchor tags.

        person_first=True  → order is  Person – Film  (acting/directing categories
                             in many modern Wikipedia layouts)
        person_first=False → order is  Film – Person  (older table layouts)
        """
        movie_title = None
        person_name = None
        is_person = self._is_person_category(category)

        for link in links:
            link_text = self._clean_text(link.get_text())
            href = link.get('href', '')
            if not link_text or href.startswith('#'):
                continue

            if person_first and is_person:
                if person_name is None:
                    person_name = link_text
                elif movie_title is None:
                    movie_title = link_text
            else:
                if movie_title is None:
                    movie_title = link_text
                elif person_name is None and is_person:
                    person_name = link_text

        return movie_title, person_name

    def _find_category_header(self, table) -> Optional[str]:
        current = table.find_previous(['h2', 'h3', 'h4'])
        if not current:
            return None
        for edit_link in current.find_all('span', class_='mw-editsection'):
            edit_link.decompose()
        headline = current.find('span', class_='mw-headline')
        if headline:
            return self._clean_text(headline.get_text())
        return self._clean_text(current.get_text())

    def _is_major_category(self, category: str) -> bool:
        category_lower = category.lower()
        for major_cat in self.MAJOR_CATEGORIES:
            if major_cat.lower() in category_lower:
                return True
        return False

    def _is_winner_row(self, row) -> bool:
        if row.find('b') or row.find('strong'):
            return True
        style = row.get('style', '').lower()
        if 'background' in style and any(
            c in style for c in ['#faeb86', '#ffc', 'gold', 'yellow', 'winner']
        ):
            return True
        text = row.get_text()
        if '🏆' in text or 'winner' in text.lower():
            return True
        first_cell = row.find(['td', 'th'])
        if first_cell:
            if 'background' in first_cell.get('style', '').lower():
                return True
            if 'winner' in ' '.join(first_cell.get('class', [])).lower():
                return True
        return False

    def _is_person_category(self, category: str) -> bool:
        person_keywords = [
            'actor', 'actress', 'director', 'writer', 'screenplay',
            'male lead', 'female lead', 'performance', 'leading role',
            'supporting role', 'supporting male', 'supporting female'
        ]
        category_lower = category.lower()
        return any(kw in category_lower for kw in person_keywords)

    def _get_person_role(self, category: str) -> str:
        cl = category.lower()
        if 'director' in cl:
            return 'Director'
        if 'screenplay' in cl or 'writer' in cl or 'writing' in cl:
            return 'Writer'
        if 'supporting' in cl or 'supporting male' in cl or 'supporting female' in cl:
            return 'Supporting Actor'
        if any(kw in cl for kw in ['actor', 'actress', 'male lead', 'female lead', 'leading role', 'performance']):
            return 'Lead Actor'
        if 'cinematography' in cl:
            return 'Cinematographer'
        if 'editing' in cl:
            return 'Editor'
        if 'score' in cl or 'song' in cl:
            return 'Composer'
        return 'Unknown'


# ======================================================================= #
# Ceremony-based scrapers                                                  #
# ======================================================================= #

class ImprovedAcademyAwardsScraper(BaseCeremonyAwardsScraper):
    """Scraper for the Academy Awards (Oscars)"""

    AWARD_NAME = "Academy Awards"
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
        'Best Documentary Feature',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape Academy Awards for ceremony year (e.g. 2024 = 96th)."""
        ceremony_number = year - 1928  # 1929 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        url = f"{self.BASE_URL}{ordinal}_Academy_Awards"
        logger.info(f"Scraping {ordinal} Academy Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping Academy Awards {year}: {e}")
            return []

    def _scrape_page(self, url: str, year: int, ceremony_number: int) -> List[Award]:
        """
        Extend base strategy chain with Oscar-specific presentation table parsing.
        Modern Oscar Wikipedia pages embed all categories inside one wikitable
        (role="presentation") using gold-background divs for headers and nested
        <ul>/<li> elements for nominees.
        """
        soup = self._fetch_page(url)

        awards = self._parse_standard_tables(soup, year, ceremony_number)
        if not awards:
            awards = self._parse_oscars_presentation_table(soup, year, ceremony_number)
        if not awards:
            awards = self._parse_by_sections(soup, year, ceremony_number)
        if not awards:
            awards = self._parse_lists(soup, year, ceremony_number)

        logger.info(f"Scraped {len(awards)} nominations/wins for {self.AWARD_NAME} {year}")
        return awards

    def _parse_oscars_presentation_table(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: int,
    ) -> List[Award]:
        """
        Parse the modern Oscar Wikipedia layout.

        Structure inside each <td>:
          <div style="...background-color:#F9EFAA...">
            <b><a>CATEGORY NAME</a></b>
          </div>
          <ul>
            <li><b>WINNER links...</b>       ← outer li = winner
              <ul>
                <li>Nominee links...</li>    ← nested li = non-winner nominees
                ...
              </ul>
            </li>
          </ul>
        """
        awards = []

        for td in soup.find_all('td'):
            cat_div = td.find(
                'div',
                style=lambda s: s and '#F9EFAA' in s
            )
            if not cat_div:
                continue

            category = self._clean_text(cat_div.get_text())
            if not self._is_major_category(category):
                continue

            ul = td.find('ul')
            if not ul:
                continue

            is_person_cat = self._is_person_category(category)

            for top_li in ul.find_all('li', recursive=False):
                is_winner = bool(top_li.find('b'))
                nested_ul = top_li.find('ul')

                # Collect links from this li BEFORE the nested ul starts
                top_links = []
                for child in top_li.children:
                    if hasattr(child, 'name') and child.name == 'ul':
                        break
                    if hasattr(child, 'find_all'):
                        top_links.extend(child.find_all('a', href=True))
                    elif hasattr(child, 'name') and child.name == 'a':
                        top_links.append(child)

                movie_title, person_name = self._extract_movie_and_person(
                    top_links, category, person_first=is_person_cat
                )
                if movie_title or person_name:
                    awards.append(Award(
                        award_name=self.AWARD_NAME,
                        ceremony_year=year,
                        ceremony_number=ceremony_number,
                        category=category,
                        movie_title=movie_title,
                        person_name=person_name,
                        person_role=self._get_person_role(category) if person_name else None,
                        won=is_winner,
                        nominated=True,
                    ))

                # Nested nominees
                if nested_ul:
                    for nested_li in nested_ul.find_all('li', recursive=False):
                        n_links = nested_li.find_all('a', href=True)
                        n_movie, n_person = self._extract_movie_and_person(
                            n_links, category, person_first=is_person_cat
                        )
                        if n_movie or n_person:
                            awards.append(Award(
                                award_name=self.AWARD_NAME,
                                ceremony_year=year,
                                ceremony_number=ceremony_number,
                                category=category,
                                movie_title=n_movie,
                                person_name=n_person,
                                person_role=self._get_person_role(category) if n_person else None,
                                won=False,
                                nominated=True,
                            ))

        return awards


class BAFTAAwardsScraper(BaseCeremonyAwardsScraper):
    """Scraper for the British Academy Film Awards (BAFTA)"""

    AWARD_NAME = "British Academy Film Awards"
    MAJOR_CATEGORIES = [
        'Best Film',
        'Best Director',
        'Best Actor in a Leading Role',
        'Best Actress in a Leading Role',
        'Best Actor in a Supporting Role',
        'Best Actress in a Supporting Role',
        'Best Original Screenplay',
        'Best Adapted Screenplay',
        'Best Animated Film',
        'Best Film Not in the English Language',
        'Best Documentary',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape BAFTA Film Awards for ceremony year (e.g. 2024 = 77th)."""
        ceremony_number = year - 1947  # 1948 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        url = f"{self.BASE_URL}{ordinal}_British_Academy_Film_Awards"
        logger.info(f"Scraping {ordinal} British Academy Film Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping BAFTA {year}: {e}")
            return []


class GoldenGlobesScraper(BaseCeremonyAwardsScraper):
    """Scraper for the Golden Globe Awards"""

    AWARD_NAME = "Golden Globe Awards"
    MAJOR_CATEGORIES = [
        'Best Motion Picture',
        'Best Director',
        # Acting — split by drama / musical or comedy
        'Best Actor in a Motion Picture',
        'Best Actress in a Motion Picture',
        'Best Supporting Actor',
        'Best Supporting Actress',
        'Best Screenplay',
        'Best Animated Feature Film',
        'Best Non-English Language Film',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape Golden Globe Awards for ceremony year (e.g. 2024 = 81st)."""
        ceremony_number = year - 1943  # 1944 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        url = f"{self.BASE_URL}{ordinal}_Golden_Globe_Awards"
        logger.info(f"Scraping {ordinal} Golden Globe Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping Golden Globes {year}: {e}")
            return []


class SAGAwardsScraper(BaseCeremonyAwardsScraper):
    """Scraper for the Screen Actors Guild Awards"""

    AWARD_NAME = "Screen Actors Guild Awards"
    MAJOR_CATEGORIES = [
        'Outstanding Performance by a Male Actor in a Leading Role',
        'Outstanding Performance by a Female Actor in a Leading Role',
        'Outstanding Performance by a Male Actor in a Supporting Role',
        'Outstanding Performance by a Female Actor in a Supporting Role',
        'Outstanding Performance by a Cast in a Motion Picture',
        # SAG uses "Outstanding Cast" for ensemble
        'Outstanding Cast',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape SAG Awards for ceremony year (e.g. 2024 = 30th)."""
        ceremony_number = year - 1994  # 1995 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        url = f"{self.BASE_URL}{ordinal}_Screen_Actors_Guild_Awards"
        logger.info(f"Scraping {ordinal} Screen Actors Guild Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping SAG Awards {year}: {e}")
            return []


class CriticsChoiceAwardsScraper(BaseCeremonyAwardsScraper):
    """Scraper for the Critics' Choice Awards"""

    AWARD_NAME = "Critics' Choice Awards"
    MAJOR_CATEGORIES = [
        'Best Picture',
        'Best Director',
        'Best Actor',
        'Best Actress',
        'Best Supporting Actor',
        'Best Supporting Actress',
        'Best Original Screenplay',
        'Best Adapted Screenplay',
        'Best Animated Feature',
        'Best Foreign Language Film',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape Critics' Choice Awards (e.g. 2024 = 29th)."""
        ceremony_number = year - 1995  # 1996 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        # Wikipedia title: "29th Critics' Choice Awards" (apostrophe encoded in URL)
        url = f"{self.BASE_URL}{ordinal}_Critics%27_Choice_Awards"
        logger.info(f"Scraping {ordinal} Critics' Choice Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping Critics' Choice {year}: {e}")
            return []


class IndependentSpiritAwardsScraper(BaseCeremonyAwardsScraper):
    """Scraper for the Film Independent Spirit Awards"""

    AWARD_NAME = "Film Independent Spirit Awards"
    MAJOR_CATEGORIES = [
        'Best Feature',
        'Best Director',
        'Best Male Lead',
        'Best Female Lead',
        'Best Supporting Male',
        'Best Supporting Female',
        'Best Screenplay',
        'Best First Screenplay',
        'Best International Film',
        'Best Documentary',
    ]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape Independent Spirit Awards (e.g. 2024 = 39th)."""
        ceremony_number = year - 1985  # 1986 = 1st
        ordinal = self._get_ordinal(ceremony_number)
        url = f"{self.BASE_URL}{ordinal}_Independent_Spirit_Awards"
        logger.info(f"Scraping {ordinal} Independent Spirit Awards ({year})")
        try:
            return self._scrape_page(url, year, ceremony_number)
        except Exception as e:
            logger.error(f"Error scraping Spirit Awards {year}: {e}")
            return []


# ======================================================================= #
# Film Festival scrapers                                                   #
# ======================================================================= #

class BaseFestivalAwardsScraper(BaseCeremonyAwardsScraper):
    """
    Base class for film festival award scrapers.
    Festivals usually have one Wikipedia page per year listing all prizes.
    """

    def _get_url(self, year: int) -> str:
        raise NotImplementedError

    def _get_ceremony_number(self, year: int) -> Optional[int]:
        return None

    def scrape_year(self, year: int) -> List[Award]:
        ceremony_number = self._get_ceremony_number(year)
        url = self._get_url(year)
        logger.info(f"Scraping {self.AWARD_NAME} {year} — {url}")
        try:
            soup = self._fetch_page(url)
            awards = self._parse_festival_page(soup, year, ceremony_number)
            if not awards:
                awards = self._parse_standard_tables(soup, year, ceremony_number)
            if not awards:
                awards = self._parse_by_sections(soup, year, ceremony_number)
            logger.info(f"Scraped {len(awards)} {self.AWARD_NAME} entries for {year}")
            return awards
        except Exception as e:
            logger.error(f"Error scraping {self.AWARD_NAME} {year}: {e}")
            return []

    def _parse_festival_page(
        self,
        soup: BeautifulSoup,
        year: int,
        ceremony_number: Optional[int]
    ) -> List[Award]:
        """
        Parse a festival awards page.  Looks for a wikitable inside an 'Awards'
        section, then falls back to searching all wikitables whose preceding
        header matches a tracked category.
        """
        awards = []

        # First, look for an explicit "Awards" section table
        awards_header = None
        for header in soup.find_all(['h2', 'h3']):
            headline = header.find('span', class_='mw-headline')
            if headline and 'award' in headline.get_text().lower():
                awards_header = header
                break

        if awards_header:
            current = awards_header.find_next_sibling()
            while current and current.name not in ['h2']:
                if current.name == 'table':
                    awards.extend(
                        self._parse_festival_awards_table(
                            current, year, ceremony_number
                        )
                    )
                current = current.find_next_sibling()

        return awards

    def _parse_festival_awards_table(
        self,
        table,
        year: int,
        ceremony_number: Optional[int]
    ) -> List[Award]:
        """
        Parse a festival awards table.  Each row is typically:
          Award Name | Film | Notes
        or
          Award Name | Person | Film
        """
        awards = []
        rows = table.find_all('tr')

        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            category_cell = cells[0]
            category = self._clean_text(category_cell.get_text())

            if not self._is_major_category(category):
                continue

            # Gather all links from remaining cells
            all_links = []
            for cell in cells[1:]:
                all_links.extend(cell.find_all('a', href=True))

            movie_title, person_name = self._extract_movie_and_person(all_links, category)

            awards.append(Award(
                award_name=self.AWARD_NAME,
                ceremony_year=year,
                ceremony_number=ceremony_number,
                category=category,
                movie_title=movie_title,
                person_name=person_name,
                person_role=self._get_person_role(category) if person_name else None,
                won=True,   # Festival tables list winners, not nominees
                nominated=True
            ))

        return awards


class CannesAwardsScraper(BaseFestivalAwardsScraper):
    """Scraper for the Cannes Film Festival"""

    AWARD_NAME = "Cannes Film Festival"
    MAJOR_CATEGORIES = [
        "Palme d'Or",
        'Grand Prix',
        'Best Director',
        'Best Actor',
        'Best Actress',
        'Best Screenplay',
        'Jury Prize',
        'Camera d\'Or',
    ]

    def _get_url(self, year: int) -> str:
        return f"{self.BASE_URL}{year}_Cannes_Film_Festival"

    def _get_ceremony_number(self, year: int) -> Optional[int]:
        # 1st Cannes was 1946; some years were skipped
        return None  # not tracked ordinally


class VeniceAwardsScraper(BaseFestivalAwardsScraper):
    """Scraper for the Venice International Film Festival"""

    AWARD_NAME = "Venice International Film Festival"
    MAJOR_CATEGORIES = [
        'Golden Lion',
        'Silver Lion',
        'Volpi Cup',
        'Best Actor',
        'Best Actress',
        'Best Screenplay',
        'Best Director',
        'Grand Jury Prize',
        'Special Jury Prize',
        'Osella',
    ]

    def _get_url(self, year: int) -> str:
        # Venice numbers: 1932 = 1st, skipping wartime/1973-74 ≈ year − 1943
        ceremony_number = year - 1943
        ordinal = self._get_ordinal(ceremony_number)
        return f"{self.BASE_URL}{ordinal}_Venice_International_Film_Festival"

    def _get_ceremony_number(self, year: int) -> int:
        return year - 1943


class BerlinAwardsScraper(BaseFestivalAwardsScraper):
    """Scraper for the Berlin International Film Festival (Berlinale)"""

    AWARD_NAME = "Berlin International Film Festival"
    MAJOR_CATEGORIES = [
        'Golden Bear',
        'Silver Bear',
        'Best Actor',
        'Best Actress',
        'Best Leading Performance',
        'Best Supporting Performance',
        'Best Screenplay',
        'Best Director',
        'Grand Jury Prize',
        'Jury Prize',
    ]

    def _get_url(self, year: int) -> str:
        # Berlin: 1951 = 1st, no significant gaps ≈ year − 1950
        ceremony_number = year - 1950
        ordinal = self._get_ordinal(ceremony_number)
        return f"{self.BASE_URL}{ordinal}_Berlin_International_Film_Festival"

    def _get_ceremony_number(self, year: int) -> int:
        return year - 1950


# ======================================================================= #
# Convenience aggregator                                                   #
# ======================================================================= #

class AllAwardsScraper:
    """
    Aggregates scraping across all supported award ceremonies and festivals.
    Focuses on acting and screenplay categories.
    """

    # Ceremonies that hold a ceremony once a year in a fixed calendar slot
    CEREMONY_SCRAPERS = [
        ImprovedAcademyAwardsScraper,
        BAFTAAwardsScraper,
        GoldenGlobesScraper,
        SAGAwardsScraper,
        CriticsChoiceAwardsScraper,
        IndependentSpiritAwardsScraper,
    ]

    # Festivals keyed by their summer/autumn timing
    FESTIVAL_SCRAPERS = [
        CannesAwardsScraper,
        VeniceAwardsScraper,
        BerlinAwardsScraper,
    ]

    def __init__(self):
        self.scrapers = [cls() for cls in self.CEREMONY_SCRAPERS + self.FESTIVAL_SCRAPERS]

    def scrape_year(self, year: int) -> List[Award]:
        """Scrape all sources for a given year."""
        all_awards: List[Award] = []
        for scraper in self.scrapers:
            try:
                awards = scraper.scrape_year(year)
                all_awards.extend(awards)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in {scraper.AWARD_NAME} scraper for {year}: {e}")
        logger.info(f"AllAwardsScraper: {len(all_awards)} total awards for {year}")
        return all_awards

    def scrape_multiple_years(self, start_year: int, end_year: int) -> List[Award]:
        """Scrape all sources across a year range."""
        all_awards: List[Award] = []
        for year in range(start_year, end_year + 1):
            all_awards.extend(self.scrape_year(year))
            time.sleep(1)
        return all_awards


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    for ScraperClass in [
        ImprovedAcademyAwardsScraper,
        BAFTAAwardsScraper,
        GoldenGlobesScraper,
        SAGAwardsScraper,
        CriticsChoiceAwardsScraper,
        IndependentSpiritAwardsScraper,
        CannesAwardsScraper,
        VeniceAwardsScraper,
        BerlinAwardsScraper,
    ]:
        scraper = ScraperClass()
        print(f"\n{'='*60}")
        print(f"Testing {scraper.AWARD_NAME} — year 2023")
        print('='*60)
        awards = scraper.scrape_year(2023)
        print(f"Total found: {len(awards)}")
        if awards:
            from collections import Counter
            categories = Counter(a.category for a in awards)
            for cat, count in categories.most_common(8):
                print(f"  {cat}: {count}")
