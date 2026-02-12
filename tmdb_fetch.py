import requests
import csv
import time
import sys
import os

# --------------------------
# CONFIGURATION
# --------------------------
API_KEY = "6e2307437dc8bf4e637ad5ae53875510"  # <-- Replace with your TMDb API Key

# --------------------------
# HELPER FUNCTIONS
# --------------------------
def search_movie(title):
    """Search TMDb for a movie and return the first result's metadata."""
    url = "https://api.themoviedb.org/3/search/movie"
    params = {
        "api_key": API_KEY,
        "query": title,
        "include_adult": False
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error searching for '{title}': {response.status_code}")
        return None
    results = response.json().get("results")
    if not results:
        print(f"No results found for '{title}'")
        return None
    return results[0]

def get_movie_details(tmdb_id):
    """Get full TMDb movie details by ID."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": API_KEY}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching details for ID {tmdb_id}")
        return None
    return response.json()

def read_movie_list(file_path):
    """Read movie titles from a text file (one per line) or CSV (first column)."""
    movies = []
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    if ext == ".txt":
        with open(file_path, "r", encoding="utf-8") as f:
            movies = [line.strip() for line in f if line.strip()]
    elif ext == ".csv":
        import pandas as pd
        df = pd.read_csv(file_path)
        movies = df.iloc[:,0].dropna().astype(str).tolist()
    else:
        print("Unsupported file type. Use .txt or .csv")
        sys.exit(1)
    return movies

# --------------------------
# MAIN SCRIPT
# --------------------------
def main():
    if len(sys.argv) < 3:
        print("Usage: python tmdb_fetch.py <input_file> <output_csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_csv = sys.argv[2]

    movies_list = read_movie_list(input_file)
    movie_data = []

    for title in movies_list:
        print(f"Processing: {title}")
        search_result = search_movie(title)
        if search_result:
            details = get_movie_details(search_result["id"])
            if details:
                movie_data.append({
                    "Title": details.get("title"),
                    "TMDb_ID": details.get("id"),
                    "Release_Date": details.get("release_date"),
                    "Genres": ", ".join([g['name'] for g in details.get("genres", [])]),
                    "Runtime": details.get("runtime"),
                    "Vote_Average": details.get("vote_average"),
                    "Vote_Count": details.get("vote_count")
                })
        time.sleep(0.25)  # avoid hitting API rate limits

    # Save to CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["Title", "TMDb_ID", "Release_Date", "Genres", "Runtime", "Vote_Average", "Vote_Count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for movie in movie_data:
            writer.writerow(movie)

    print(f"CSV file '{output_csv}' created successfully with {len(movie_data)} movies.")

if __name__ == "__main__":
    main()
