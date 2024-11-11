import os
import csv
import time
from dotenv import load_dotenv
from klaviyo_api import KlaviyoAPI
from typing import List, Dict

# Load environment variables from .env file
load_dotenv()

# Initialize Klaviyo API client
klaviyo_api_key = os.getenv("KLAVIYO_API_KEY")
klaviyo = KlaviyoAPI(klaviyo_api_key)


def delete_profiles_from_csv(csv_file: str) -> None:
    """
    Deletes profiles listed in a CSV file from Klaviyo via the API. After each successful deletion,
    the profile ID is removed from the CSV file, allowing for safe interruption and continuation of
    the deletion process.

    Args:
        csv_file (str): The path to the CSV file containing profile IDs under the "profile_id" column.

    Returns:
        None
    """
    # Read profile IDs from the CSV
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        profiles: List[Dict[str, str]] = list(reader)

    # Loop over each profile
    for row in profiles:
        profile_id = row.get("profile_id")

        if profile_id:
            # Build request payload
            body = {
                "data": {
                    "type": "data-privacy-deletion-job",
                    "attributes": {
                        "profile": {
                            "data": {
                                "type": "profile",
                                "id": profile_id,
                                "attributes": {
                                    "id": profile_id
                                }
                            }
                        }
                    }
                }
            }

            try:
                # Request profile deletion
                klaviyo.Data_Privacy.request_profile_deletion(body)
                print(f"Successfully deleted Profile ID: {profile_id}")
                time.sleep(1.02)  # Rate limit control: up to 60/min, wait 1 sec between requests

                # Remove the deleted profile from the list
                profiles = [p for p in profiles if p.get("profile_id") != profile_id]

                # Update CSV file with remaining profiles
                with open(csv_file, mode="w", newline="") as file:
                    fieldnames = ["profile_id"]
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(profiles)
                    file.flush()  # Ensures data is written to disk immediately
                    os.fsync(file.fileno())  # Ensures data is written to disk in real time

            except Exception as e:
                print(f"Failed to delete Profile ID: {profile_id}")
                print("Error:", str(e))
                # Keep the profile in the profiles list if deletion fails
                continue

    print("Deletion process completed.")


# Run the deletion process for profiles listed in unique_master_profiles.csv
delete_profiles_from_csv("unique_master_profiles.csv")
