import requests
import sys
import json
import time

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
        print(json.dumps({"error": f"Error searching for '{title}': {response.status_code}"}))
        return None
    results = response.json().get("results")
    if not results:
        print(json.dumps({"error": f"No results found for '{title}'"}))
        return None
    return results[0]

def get_movie_details(tmdb_id):
    """Get full TMDb movie details by ID, excluding images, keywords, recommendations, and watch providers."""
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": API_KEY,
        # Include everything except the excluded categories
        "append_to_response": "credits,release_dates,external_ids"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(json.dumps({"error": f"Error fetching details for ID {tmdb_id}"}))
        return None
    return response.json()

# --------------------------
# MAIN SCRIPT
# --------------------------
def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python tmdb_fetch.py \"Movie Title\""}))
        sys.exit(1)

    title = sys.argv[1]
    search_result = search_movie(title)
    if not search_result:
        sys.exit(1)

    details = get_movie_details(search_result["id"])
    if not details:
        sys.exit(1)

    # Output JSON
    print(json.dumps(details, indent=4))

    # Optional: brief delay for rate limiting
    time.sleep(0.25)

if __name__ == "__main__":
    main()
