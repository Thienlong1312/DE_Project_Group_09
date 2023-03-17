from dagster import op

@op

def extract_from_api():
    return "Extracted data from API"