import requests
import csv
import time
import tmdb_fetch as tf

# --------------------------
# CONFIGURATION
# --------------------------
API_KEY = tf.API_KEY
MOVIES = [
    "Oppenheimer",
    "Barbie",
    "Past Lives",
    "American Fiction",
    "Killers of the Flower Moon"
    # Add more movie titles here
]
OUTPUT_CSV = "tmdb_best_picture.csv"

# --------------------------
# MAIN SCRIPT
# --------------------------
movie_data = []

for title in MOVIES:
    print(f"Processing: {title}")
    search_result = tf.search_movie(title)
    if search_result:
        details = tf.get_movie_details(search_result["id"])
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

# --------------------------
# SAVE TO CSV
# --------------------------
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["Title", "TMDb_ID", "Release_Date", "Genres", "Runtime", "Vote_Average", "Vote_Count"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for movie in movie_data:
        writer.writerow(movie)

print(f"CSV file '{OUTPUT_CSV}' created successfully with {len(movie_data)} movies.")