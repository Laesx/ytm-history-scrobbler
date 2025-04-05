#!/usr/bin/env python3
import argparse
import json
import os
from ytmusicapi import OAuthCredentials, YTMusic
from typing import Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()

# CLIENT_ID = os.getenv("CLIENT_ID")
# CLIENT_SECRET = os.getenv("CLIENT_SECRET")


class YouTubeMusicHistoryProcessor:
    def __init__(self):

        if not os.path.exists("oauth.json"):
            self.authenticated = False
            self.ytmusic = YTMusic()
        else:
            self.authenticated = True
            self.ytmusic = YTMusic("oauth.json", oauth_credentials=OAuthCredentials(client_id=os.getenv("CLIENT_ID"), client_secret=os.getenv("CLIENT_SECRET")))
        # self.ytmusic = YTMusic(browser_auth_file)
        self.watch_history_file = "watch-history.json"
        self.cache_file = "api_cache.json"
        self.library_uploads_cache = None
        self.queried_ids = set()
        self.successful_api_count = 0
        self.songs = []
        self.api_cache = {}
        self.load_cache()

    def load_cache(self):
        """Load the API cache from file if it exists"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "r", encoding="utf-8") as file:
                    self.api_cache = json.load(file)
                print(f"Loaded cache with {len(self.api_cache)} entries")
                # Load previously queried IDs
                self.queried_ids = set(self.api_cache.keys())
        except Exception as e:
            print(f"Error loading cache file: {e}")
            self.api_cache = {}

    def save_cache(self):
        """Save the current API cache to file"""
        try:
            with open(self.cache_file, "w", encoding="utf-8") as file:
                json.dump(self.api_cache, file, indent=2, ensure_ascii=False)
            # print(f"Saved API cache with {len(self.api_cache)} entries")
        except Exception as e:
            print(f"Error saving cache file: {e}")

    def read_watch_history(self) -> List[Dict]:
        """Read and parse the YouTube Music watch history file"""
        print("\nReading YouTube Music history from watch-history.json\n")
        try:
            with open(self.watch_history_file, "r", encoding="utf-8") as file:
                return json.load(file)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading watch history file: {e}")
            return []

    def extract_artist_name(self, item: Dict) -> Optional[str]:
        """Extract artist name from item subtitles"""
        if not item.get("subtitles"):
            return None
            
        try:
            if isinstance(item["subtitles"], list) and item["subtitles"]:
                return item["subtitles"][0].get("name", "")
            elif isinstance(item["subtitles"], dict):
                return item["subtitles"].get("name", "")
            else:
                subtitles_str = json.dumps(item.get("subtitles"))
                if "," in subtitles_str:
                    return subtitles_str.split(",")[0][10:-1]
        except Exception as e:
            print(f"Error parsing subtitle: {e}")
            print(f"Subtitle content: {item.get('subtitles')}")
        
        return None

    def process_history_data(self):
        """Parse the YouTube Music watch history and extract relevant song information"""
        parsed_data = self.read_watch_history()
        
        for item in parsed_data:
            if item.get("header") != "YouTube Music":
                continue
                
            raw_artist = self.extract_artist_name(item)
            if not raw_artist:
                continue
                
            # Only process Topic channels or Library Uploads
            if " - Topic" not in raw_artist and "Music Library Uploads" not in raw_artist:
                continue
                
            # Process valid music entry
            artist = raw_artist[:-8] if " - Topic" in raw_artist else raw_artist
            artist = artist.replace('\\"', '"').replace('\\\"', '"')  # Clean quotes
            
            title = item.get("title", "").replace("Watched ", "")
            song_id = item.get("titleUrl", "").split("=")[-1] if item.get("titleUrl") else ""
            timestamp = item.get("time", "")
            
            song = {
                "artistName": artist,
                "trackName": title,
                "ts": timestamp,
                "id": song_id,
                "isLibraryUpload": "Music Library Uploads" in raw_artist
            }
            
            self.songs.append(song)
        
        print(f"Found {len(self.songs)} YouTube Music songs in watch-history.json")

    def get_library_uploads(self) -> List[Dict]:
        """Fetch all library uploads from the YouTube Music account"""

        if not self.authenticated:
            print("""WARNING: You're not authenticated but library uploads were found in the watch history, 
                    consider authenticating to get all possible entries, skipping library uploads""")
            return []

        if self.library_uploads_cache is not None:
            return self.library_uploads_cache
        
        cache_key = "library_uploads"
        if cache_key in self.api_cache:
            self.library_uploads_cache = self.api_cache[cache_key]
            print(f"Using cached library uploads ({len(self.library_uploads_cache)} items)")
            return self.library_uploads_cache

        print("Fetching all library uploads...")
        try:
            self.library_uploads_cache = self.ytmusic.get_library_upload_songs(limit=None)
            self.successful_api_count += 1
            print(f"Successfully fetched {len(self.library_uploads_cache)} library uploads")
            
            # Save to cache
            self.api_cache[cache_key] = self.library_uploads_cache
            self.save_cache()

            return self.library_uploads_cache
        except Exception as e:
            print(f"Error fetching library uploads: {e}")
            return []

    def find_upload_match(self, id: str) -> Optional[Dict]:
        """Search through library uploads to find a matching song"""
        uploads = self.get_library_uploads()
        
        # Normalize search terms for better matching
        
        for upload in uploads:
            # Extract upload artist and title for comparison
            # upload_artist = upload.get('artists', [{}])[0].get('name', '').lower() if upload.get('artists') else ''
            # upload_title = upload.get('title', '').lower()
            
            # Check for match (flexible matching for slight naming differences)
            if id == upload.get('videoId'):
                
                # Get artist name from the upload if available
                actual_artist = upload.get('artists', [{}])[0].get('name', '') if upload.get('artists') else 'Unknown'
                
                # Extract album name correctly based on the structure
                album_name = ''
                if isinstance(upload.get('album'), dict):
                    album_name = upload.get('album', {}).get('name', '')
                else:
                    album_name = upload.get('album', '')
                    
                return {
                    'albumName': album_name,
                    'videoId': upload.get('videoId', ''),
                    'artistName': actual_artist
                }
        
        return None

    def process_library_upload(self, song: Dict, index: int) -> bool:
        """Process a library upload song to find album info"""
        upload_match = self.find_upload_match(song['id'])
        if not upload_match:
            return False
            
        self.songs[index]['albumName'] = upload_match['albumName']
        self.songs[index]['artistName'] = upload_match['artistName']
        
        return True

    def process_regular_song(self, song: Dict, index: int) -> bool:
        """Process a regular (non-library) song to find album info"""
        search_query = f"{song['artistName']} - {song['trackName']}"
        
        # Check cache for this query
        song_id = song.get('id', '')
        if song_id and song_id in self.api_cache:
            cached_data = self.api_cache[song_id]
            if 'albumName' in cached_data:
                self.songs[index]['albumName'] = cached_data['albumName']
                return True
        

        
        search_results = self.ytmusic.search(search_query, filter="songs", limit=1)
        self.successful_api_count += 1
        
        if search_results and 'album' in search_results[0]:
            album_name = search_results[0]['album']['name']
            self.songs[index]['albumName'] = album_name

            # Special case for Release (Some songs don't have and artist page so they are listed as "Release" in the watch history)
            if song['artistName'] == "Release" and 'artists' in search_results[0]:
                self.songs[index]['artistName'] = search_results[0]['artists'][0]['name']
            
            # Cache the result
            if song_id:
                self.api_cache[song_id] = {
                    'albumName': album_name,
                    'artistName': song.get('artistName', '')
                }
                # Save cache periodically (every 10 API calls)
                if self.successful_api_count % 10 == 0:
                    self.save_cache()
                    
            return True
            
        return False

    def fetch_album_info(self):
        """Fetch album information for all songs"""
        print(f"Processing all {len(self.songs)} songs")
        completed_items = 0
        
        for i, song in enumerate(self.songs):
            song_id = song.get('id', '')
            
            # Check if we already have info for this ID
            if song_id and song_id in self.queried_ids and song_id in self.api_cache:
                # Reuse the cached album info
                cached_data = self.api_cache[song_id]
                if 'albumName' in cached_data:
                    self.songs[i]['albumName'] = cached_data['albumName']
                if 'artistName' in cached_data:
                    self.songs[i]['artistName'] = cached_data['artistName']
                completed_items += 1
                continue
            
            success = False
            try:
                # Process based on song type
                if song.get('isLibraryUpload', False):
                    success = self.process_library_upload(song, i)
                else:
                    success = self.process_regular_song(song, i)
                
                # Update successful count and record queried ID
                if success and song_id:
                    self.queried_ids.add(song_id)
                    self.api_cache[song_id] = {
                        'albumName': song.get('albumName', ''),
                        'artistName': song.get('artistName', '')
                    }
                    
                    # Save cache periodically (every 10 completed items)
                    # if completed_items % 10 == 0:
                    #     self.save_cache()
                    
            except Exception as e:
                print(f"Error processing item {i}: {e}")
                # Save cache on error to preserve progress
                self.save_cache()
            
            completed_items += 1
            
            # Show progress periodically
            if completed_items % 10 == 0 or completed_items == len(self.songs):
                print(f"Progress: {completed_items}/{len(self.songs)} "
                      f"({self.successful_api_count} API Requests)")
        
        # Final cache save before finalizing data
        self.save_cache()
        self.finalize_data()

    def finalize_data(self):
        """Finalize the data and write it to disk"""
        print(f"Finished with {self.successful_api_count} successful API requests.")
        
        # Remove temporary fields from each item
        for song in self.songs:
            song.pop('id', None)
            song.pop('isLibraryUpload', None)
        
        self.write_output_files()
    
    def write_output_files(self):
        """Write the processed data to output files"""
        # Write file(s) based on size
        if len(self.songs) < 2800:
            with open("formatted.json", "w", encoding="utf-8") as file:
                json.dump(self.songs, indent=2, ensure_ascii=False, fp=file)
        else:
            chunk_size = 2800
            for i, chunk_start in enumerate(range(0, len(self.songs), chunk_size)):
                chunk = self.songs[chunk_start:chunk_start + chunk_size]
                with open(f"formatted-{i+1}.json", "w", encoding="utf-8") as file:
                    json.dump(chunk, indent=2, ensure_ascii=False, fp=file)
        
        print("\nFinished Successfully, final file(s) written to formatted.json")
        print("Download and open Last.FM-Scrubbler-WPF")
        print("https://github.com/SHOEGAZEssb/Last.fm-Scrubbler-WPF")
        print("Select 'File Parse Scrobbler', change the Parser to JSON, import formatted.json and click Parse")
        print(":)")

    def write_test_file(self):
        """Write intermediate test file with current data"""
        with open("formatted-test.json", "w", encoding="utf-8", errors="replace") as file:
            json.dump(self.songs, indent=2, ensure_ascii=False, fp=file)
        print("Test file written to formatted-test.json")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process YouTube Music watch history')
    parser.add_argument('--limit', type=int, help='Limit the number of songs to process')
    # parser.add_argument('--no-album', action='store_true', help='Do not fetch album information')
    parser.add_argument('--only-uploads', action='store_true', help='Only process library uploads')
    parser.add_argument('--test-mode', action='store_true', help='Run in test mode (limit to 500 songs)')
    args = parser.parse_args()

    # Create processor and process data
    processor = YouTubeMusicHistoryProcessor()
    processor.process_history_data()

    # Apply filters
    if args.limit:
        processor.songs = processor.songs[:args.limit]
    
    if args.only_uploads:
        processor.songs = [song for song in processor.songs if song.get('isLibraryUpload', False)]

    # Apply test mode limit
    if args.test_mode:
        processor.songs = processor.songs[:500]
        print("Running in test mode - limited to 500 songs")

    # Write test file with data so far
    # processor.write_test_file()

    processor.fetch_album_info()

if __name__ == "__main__":
    main() 