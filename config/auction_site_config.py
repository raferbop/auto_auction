import os
from dotenv import load_dotenv
load_dotenv()
# Auction sites configuration
auction_sites = {
    "AutoPacific": {
        "username": os.environ.get("AUTOPACIFIC_USERNAME"),
        "password": os.environ.get("AUTOPACIFIC_PASSWORD"),
        "scraping": {
            "auction_url": "https://auction.pacificcoastjdm.com/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auction.pacificcoastjdm.com/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Zervtek": {
        "username": os.environ.get("ZERVTEK_USERNAME"),
        "password": os.environ.get("ZERVTEK_PASSWORD"),
        "scraping": {
            "auction_url": "https://auctions.zervtek.com/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auctions.zervtek.com/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Manga Auto Import": {
        "username": os.environ.get("MANGA_AUTO_IMPORT_USERNAME"),
        "password": os.environ.get("MANGA_AUTO_IMPORT_PASSWORD"),
        "scraping": {
            "auction_url": "https://auc.mangaautoimport.ca/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auc.mangaautoimport.ca/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Japan Car Auc": {
        "username": os.environ.get("JAPAN_CAR_AUC_USERNAME"),
        "password": os.environ.get("JAPAN_CAR_AUC_PASSWORD"),
        "scraping": {
            "auction_url": "https://auc.japancarauc.com/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auc.japancarauc.com/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    },
    "Zen Autoworks": {
        "username": os.environ.get("ZEN_AUTOWORKS_USERNAME"),
        "password": os.environ.get("ZEN_AUTOWORKS_PASSWORD"),
        "scraping": {
            "auction_url": "https://auction.zenautoworks.ca/auctions/?p=project/searchform&searchtype=max&s&ld",
            "sales_data_url": "https://auction.zenautoworks.ca/stats/?p=project/searchform&searchtype=max&s&ld"
        }
    }
} 