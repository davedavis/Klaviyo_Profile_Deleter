import os
from dotenv import load_dotenv
from klaviyo_api import KlaviyoAPI
import csv
from typing import Set, List

# Load environment variables from .env file
load_dotenv()

# Initialize Klaviyo API client
klaviyo_api_key = os.getenv("KLAVIYO_API_KEY")
klaviyo = KlaviyoAPI(klaviyo_api_key)

# List IDs
master_lists = ["asdfasdf", "3g535g"]
marketing_lists = ["ergheth", "rtherhth", "wrthreth", "wthwhtrwh", "wtehwtht", "wethwth", "wethweth", "34tqradf", "EFWAF4W"]

def fetch_list_profiles(list_id: str, output_file: str) -> Set[str]:
    """
    Fetch all profile IDs from a specified list in Klaviyo and save them to a CSV file.

    Args:
        list_id (str): The Klaviyo list ID to fetch profiles from.
        output_file (str): The name of the CSV file to save the profile IDs.

    Returns:
        Set[str]: A set of profile IDs retrieved from the specified list.
    """
    page_cursor = None
    has_more = True
    profiles_retrieved = 0
    profile_ids = []

    while has_more:
        response = klaviyo.Lists.get_list_profiles(
            list_id,
            fields_profile=["id"],  # Only fetch profile IDs to reduce payload
            page_size=100,
            page_cursor=page_cursor
        )

        # Process profiles in the current page
        profiles = response.get("data", [])
        for profile in profiles:
            profile_id = profile.get("id")
            profile_ids.append(profile_id)
            profiles_retrieved += 1

        # Get the next cursor and check if more pages are available
        page_cursor = response.get("links", {}).get("next")
        has_more = page_cursor is not None

    # Write profile IDs to CSV
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["profile_id"])
        for profile_id in profile_ids:
            writer.writerow([profile_id])

    print(f"Total profiles retrieved from list {list_id}: {profiles_retrieved}")
    return set(profile_ids)

# Step 1: Fetch profiles for each marketing list and save to CSV
marketing_profiles: Set[str] = set()
for list_id in marketing_lists:
    marketing_profiles.update(fetch_list_profiles(list_id, f"{list_id}_marketing_profiles.csv"))

# Step 2: Fetch profiles for each master list and save to CSV
master_profiles: Set[str] = set()
for list_id in master_lists:
    master_profiles.update(fetch_list_profiles(list_id, f"{list_id}_master_profiles.csv"))

# Step 3: Calculate profiles in master lists that are not in marketing lists
unique_master_profiles: Set[str] = master_profiles - marketing_profiles

# Save the final list to a CSV
with open("unique_master_profiles.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["profile_id"])
    for profile_id in unique_master_profiles:
        writer.writerow([profile_id])

print(f"Total unique profiles in master lists not in marketing lists: {len(unique_master_profiles)}")
