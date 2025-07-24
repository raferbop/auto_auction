import csv
import string
import random
from datetime import datetime

# Load your manufacturer config
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from manufacturer_config import manufacturer_configs

# Email settings
EMAIL_PROVIDERS = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]

def clean_string(text):
    """Remove spaces/special chars to make email-safe."""
    return ''.join(e for e in text if e.isalnum()).upper()

def generate_email(model, make):
    """Format: model + make + 'jmd' + @domain."""
    model_clean = clean_string(model)
    make_clean = clean_string(make)
    domain = random.choice(EMAIL_PROVIDERS)
    return f"{model_clean}{make_clean}jmd@{domain}"

def generate_email_mapping():
    """Create a mapping of make-model pairs to structured emails."""
    email_mapping = []
    for manufacturer, data in manufacturer_configs.items():
        for make, models in data["makes"].items():
            for model in models:
                email = generate_email(model, make)
                email_mapping.append({
                    "manufacturer": manufacturer.upper(),
                    "make": make,
                    "model": model,
                    "email": email
                })
    return email_mapping

if __name__ == "__main__":
    # Generate all emails
    email_mapping = generate_email_mapping()
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"make_model_emails_jmd_{timestamp}.csv"
    
    with open(filename, "w", newline="") as csvfile:
        fieldnames = ["manufacturer", "make", "model", "email"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(email_mapping)
    
    print(f"Generated {len(email_mapping)} emails. Saved to: {filename}") 