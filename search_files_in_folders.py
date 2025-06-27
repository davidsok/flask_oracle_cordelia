import glob
import os
import re

def find_files_by_pattern(directory, pattern):
    """
    Finds files in a given directory that match a specified pattern.

    Args:
        directory (str): The path to the directory to search.
        pattern (str): The filename pattern to match (e.g., "*.txt", "image_*.png").

    Returns:
        list: A list of full paths to the matching files.
    """
    search_path = os.path.join(directory, "**", pattern)
    matching_files = glob.glob(search_path, recursive=True)
    return matching_files

# Example usage:
folder_to_search = "N:\\data\\picture"  # Replace with your directory
file_pattern = "A*.jpg"  # Example: search for all .log files

found_files = find_files_by_pattern(folder_to_search, file_pattern)

if found_files:
    print(f"Files matching '{file_pattern}' in '{folder_to_search}':")
    for file_path in found_files:
        print(file_path)
else:
    print(f"No files matching '{file_pattern}' found in '{folder_to_search}'.")