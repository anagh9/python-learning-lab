# Problem 1

# Client data arrives with inconsistent date formats: "01-Jan-2024", "2024/01/01", "Jan 1 2024". Normalize all to ISO format.

from datetime import datetime


def normalize_dates(dates):
    """
    Normalize dates in various formats to ISO format (YYYY-MM-DD).
    
    Args:
        dates: List of date strings in various formats
        
    Returns:
        List of dates in ISO format
    """
    # Common date formats to try
    date_formats = [
        "%d-%b-%Y",      # "01-Jan-2024"
        "%Y/%m/%d",      # "2024/01/01"
        "%b %d %Y",      # "Jan 01 2024"
        "%B %d %Y",      # "January 01 2024"
        "%m/%d/%Y",      # "01/01/2024"
        "%d/%m/%Y",      # "01/01/2024"
        "%Y-%m-%d",      # "2024-01-01"
    ]
    
    normalized = []
    
    for date_str in dates:
        date_str = date_str.strip()
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                iso_date = parsed_date.strftime("%Y-%m-%d")
                normalized.append(iso_date)
                break
            except ValueError:
                continue
        else:
            # If no format matches, try dateutil parser as fallback
            try:
                from dateutil import parser
                parsed_date = parser.parse(date_str)
                iso_date = parsed_date.strftime("%Y-%m-%d")
                normalized.append(iso_date)
            except Exception:
                # If all parsing fails, append original and mark as failed
                normalized.append(f"FAILED: {date_str}")
    
    return normalized


# Test cases
if __name__ == "__main__":
    test_dates = [
        "01-Jan-2024",
        "2024/01/01",
        "Jan 1 2024",
        "January 15 2024",
        "01/15/2024",
        "2024-03-20",
    ]
    
    results = normalize_dates(test_dates)
    
    print("Date Normalization Results:")
    print("-" * 50)
    for original, normalized in zip(test_dates, results):
        print(f"{original:20} -> {normalized}")

