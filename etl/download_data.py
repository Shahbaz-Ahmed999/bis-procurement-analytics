import os
import requests

# All monthly CSV files from the BIS dataset (April 2015 - July 2016 focus)
files = {
    "april_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/453730/April_2015_published.csv",
    "may_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/453733/May_2015_published.csv",
    "june_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/465774/BIS_June_2015.csv",
    "july_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/465776/BIS_July_2015.csv",
    "august_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/473265/BIS-spending-august-2015.csv",
    "september_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/482020/BIS-spending-september-2015.csv",
    "october_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497020/BIS-spending-october-2015.csv",
    "november_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/497022/BIS-spending-november-2015.csv",
    "december_2015.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/504010/BIS-spending-December-2015.csv",
    "january_2016.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/518721/bis-spending-january-2016.csv",
    "february_2016.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/526455/bis-spending-february-2016.csv",
    "march_2016.csv": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/529539/bis-spending-march-2016.csv",
}

save_dir = "data/raw"
os.makedirs(save_dir, exist_ok=True)

for filename, url in files.items():
    save_path = os.path.join(save_dir, filename)
    print(f"Downloading {filename}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print(f"  ✓ Saved to {save_path}")
    else:
        print(f"  ✗ Failed ({response.status_code}): {url}")

print("\nDone.")