from pathlib import Path
from dotenv import load_dotenv
import sys

# Load variables from .env into os.environ
load_dotenv()

# Delay this import if possible, or ensure load_dotenv() runs first
import kaggle 

# Use Pathlib for robust path handling (Fluent Python style)
BASE_DIR = Path(__file__).resolve().parent.parent
FILEPATH = BASE_DIR / "data"

def download_raw_data(dataset_id: str, target_path: Path):
    """Downloads and unzips a Kaggle dataset."""

    try:
        # authenticate() automatically looks for KAGGLE_USERNAME and KAGGLE_KEY in os.environ
        kaggle.api.authenticate()
        print("Authentication successful.")
        kaggle.api.dataset_download_files(
            dataset_id, 
            path=str(target_path), 
            unzip=True
        )
        print(f"Dataset {dataset_id} extracted to: {target_path}")
        
    except Exception as e:
        print(f"Failed to download dataset: {e}")
        sys.exit(1) # Added exit for failure case 

if __name__ == "__main__":
    download_raw_data("olistbr/brazilian-ecommerce", FILEPATH)