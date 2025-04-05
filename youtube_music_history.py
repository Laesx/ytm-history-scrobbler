#!/usr/bin/env python3
import argparse
import json
import os
import time
from ytmusicapi import YTMusic
from typing import Dict, Set, List, Literal

# Initialize YTMusic API client
ytmusic = YTMusic("browser.json")

# This is the array that will eventually be written to the JSON
# global formatted_data
formatted_data = []
# Cache for already queried IDs to avoid duplicate API calls
queried_ids = set()
# Cache for library upload songs (will be populated on demand)
library_uploads_cache = None

def process_history_data():
    """Parse the YouTube Music watch history and extract relevant song information"""
    print("\n\tFiltering initial file for only YouTube Music results\n")

    global formatted_data
    
    # Read the watch history data
    with open("watch-history.json", "r", encoding="utf-8") as file:
        parsed_data = json.load(file)
    
    # Process entries from the watch history
    for item in parsed_data:
        if item.get("header") == "YouTube Music":
            new_obj = {}
            
            # Extract artist information from subtitles
            raw_artist = None
            if item.get("subtitles"):
                # Better extraction of artist name
                try:
                    # Try to get the 'name' field directly
                    if isinstance(item["subtitles"], list) and len(item["subtitles"]) > 0:
                        raw_artist = item["subtitles"][0].get("name", "")
                    # Fallback to the string parsing method if needed
                    elif isinstance(item["subtitles"], dict):
                        raw_artist = item["subtitles"].get("name", "")
                    else:
                        # Convert subtitles to string and extract artist name
                        subtitles_str = json.dumps(item.get("subtitles"))
                        if "," in subtitles_str:
                            raw_artist = subtitles_str.split(",")[0][10:-1]
                except Exception as e:
                    print(f"Error parsing subtitle: {e}")
                    print(f"Subtitle content: {item.get('subtitles')}")
                    continue
            
            if raw_artist:
                # Handle regular artist songs
                if " - Topic" in raw_artist or "Music Library Uploads" in raw_artist:
                    if " - Topic" in raw_artist:
                        artist = raw_artist[:-8]  # Remove " - Topic" suffix
                    else:
                        artist = raw_artist
                    title = item.get("title", "").replace("Watched ", "")
                    song_id = item.get("titleUrl", "").split("=")[-1] if item.get("titleUrl") else ""
                    timestamp = item.get("time", "")
                    
                    # Ensure artist name doesn't have extra quotes
                    artist = artist.replace('\\"', '"').replace('\\\"', '"')
                    
                    new_obj["artistName"] = artist
                    new_obj["trackName"] = title
                    new_obj["ts"] = timestamp
                    new_obj["id"] = song_id
                    new_obj["isLibraryUpload"] = "Music Library Uploads" in raw_artist
                    
                    formatted_data.append(new_obj)

def get_library_uploads() -> List[Dict]:
    """Fetch all library uploads from the YouTube Music account"""
    global library_uploads_cache
    
    if library_uploads_cache is not None:
        return library_uploads_cache
    
    print("Fetching all library uploads...")
    try:
        # Fetch all library uploads at once
        library_uploads_cache = ytmusic.get_library_upload_songs(limit=None)
        print(f"Successfully fetched {len(library_uploads_cache)} library uploads")
        return library_uploads_cache
    except Exception as e:
        print(f"Error fetching library uploads: {e}")
        return []

def find_upload_match(artist_name: str, track_name: str) -> Dict:
    """Search through library uploads to find a matching song"""
    uploads = get_library_uploads()
    
    # Normalize search terms for better matching
    artist_name_lower = artist_name.lower()
    track_name_lower = track_name.lower()
    
    for upload in uploads:
        # Extract upload artist and title for comparison
        upload_artist = upload.get('artists', [{}])[0].get('name', '').lower() if upload.get('artists') else ''
        upload_title = upload.get('title', '').lower()
        
        # Check for match (flexible matching to account for slight naming differences)
        if (artist_name_lower in upload_artist or upload_artist in artist_name_lower) and \
           (track_name_lower in upload_title or upload_title in track_name_lower):
            return {
                'albumName': upload.get('album', ''),
                'videoId': upload.get('videoId', '')
            }
    
    return None

def fetch_album_info():
    """Fetch album information for each song in the formatted data"""
    index_increment = 20
    last_index = 0
    cur_index = 0
    successful_apis = 0
    
    def batch_api(index):
        """Process a batch of API requests"""
        nonlocal last_index, cur_index, successful_apis

        global formatted_data
        
        cur_index = index
        print(f"Api Querying items {last_index} to {cur_index}")
        
        # Track items being processed and completed for progress reporting
        items_to_process = min(cur_index - last_index + 1, len(formatted_data) - last_index)
        completed_items = 0
        
        # Process each item in the current batch
        for i in range(last_index, min(cur_index + 1, len(formatted_data))):
            try:
                item = formatted_data[i]
                song_id = item.get('id', '')
                
                # Skip if this ID has already been queried
                if song_id and song_id in queried_ids:
                    print(f"Skipping already queried ID: {song_id}")
                    completed_items += 1
                    continue
                
                # Handle library uploads differently
                if item.get('isLibraryUpload', False):
                    upload_match = find_upload_match(item['artistName'], item['trackName'])
                    if upload_match:
                        formatted_data[i]['albumName'] = upload_match['albumName']
                        successful_apis += 1
                        
                        # Add to queried IDs
                        if song_id:
                            queried_ids.add(song_id)
                else:
                    # Regular API search for non-library songs
                    search_query = f"{item['artistName']} - {item['trackName']}"
                    search_results = ytmusic.search(search_query, filter="songs", limit=1)
                    
                    if search_results and len(search_results) > 0:
                        if 'album' in search_results[0]:
                            formatted_data[i]['albumName'] = search_results[0]['album']['name']
                            successful_apis += 1
                    
                    # Add to queried IDs
                    if song_id:
                        queried_ids.add(song_id)
                
                completed_items += 1
                
                # Show progress after every 5 items
                if completed_items % 5 == 0 or completed_items == items_to_process:
                    print(f"Progress: {completed_items}/{items_to_process} ({successful_apis} album names found)")
                
                # Add a small delay to avoid rate limiting (only for regular API calls)
                # if not item.get('isLibraryUpload', False):
                #     time.sleep(0.2)
                
            except Exception as e:
                print(f"Error processing item {i}: {e}")
        
        # Move to the next batch
        last_index = cur_index + 1
        
        # Check if we need to process more batches
        if last_index < len(formatted_data):
            next_index = min(cur_index + index_increment, len(formatted_data) - 1)
            batch_api(next_index)
        else:
            finish_processing()
    
    # Start processing with the first batch
    batch_api(min(index_increment - 1, len(formatted_data) - 1))

def finish_processing():
    """Finalize the data and write it to disk"""
    global formatted_data, successful_apis
    
    print(f"Finished with {successful_apis} successful API requests.")
    # print("Waiting 5 seconds for last requests to complete before writing to file")
    
    # time.sleep(5)
    
    # Remove ID field from each item
    for item in formatted_data:
        if 'id' in item:
            del item['id']
        if 'isLibraryUpload' in item:
            del item['isLibraryUpload']
    
    # Write file(s) based on size
    if len(formatted_data) < 2800:
        with open("formatted.json", "w", encoding="utf-8") as file:
            json.dump(formatted_data, indent=2, ensure_ascii=False, fp=file)
    else:
        file_num = 0
        formatted_data_copy = formatted_data.copy()
        while formatted_data_copy:
            file_num += 1
            arr_section = formatted_data_copy[:2800]
            formatted_data_copy = formatted_data_copy[2800:]
            
            with open(f"formatted-{file_num}.json", "w", encoding="utf-8") as file:
                json.dump(arr_section, indent=2, ensure_ascii=False, fp=file)
    
    print("\nFinished Successfully, final file(s) called formatted.json")
    print("Download and open Last.FM-Scrubbler-WPF")
    print("https://github.com/SHOEGAZEssb/Last.fm-Scrubbler-WPF")
    print("Select 'File Parse Scrobbler', change the Parser to JSON, import formatted.json and click Parse")
    print(":)")

if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(description='Process YouTube Music watch history')
    parser.add_argument('--limit', type=int, help='Limit the number of songs to process')
    parser.add_argument('--no-album', action='store_true', help='Do not fetch album information')
    parser.add_argument('--only-uploads', action='store_true', help='Only process library uploads')
    args = parser.parse_args()

    # Process the watch history data
    process_history_data()

    if args.limit:
        formatted_data = formatted_data[:args.limit]
    
    if args.only_uploads:
        formatted_data = [item for item in formatted_data if item.get('isLibraryUpload', False)]

    # Limit to 500 for testing (comment or remove this in production)
    formatted_data = formatted_data[:500]

    print(f"\n\tFound {len(formatted_data)} Youtube Music songs in watch-history.json")
    print("\twatch-history.json does not have album names, grabbing them from Youtube API (only 90% success rate)")
    print("\tIf the program stops before it says file written, an error occurred, just close and re-run\n")

    # Write test file with the data so far
    with open("formatted-test.json", "w", encoding="utf-8", errors="replace") as file:
        json.dump(formatted_data, indent=2, ensure_ascii=False, fp=file)
    
    # Fetch album information for each song
    successful_apis = 0
    
    if not args.no_album:
        fetch_album_info()
    else:
        finish_processing() 