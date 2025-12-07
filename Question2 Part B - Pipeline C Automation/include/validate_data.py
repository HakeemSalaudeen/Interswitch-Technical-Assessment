import logging
from datetime import datetime

log = logging.getLogger(__name__)

def _validate(**context):
    df = context['ti'].xcom_pull(task_ids='extract_data')
    log.info(f"Pulled data with {len(df)} records")

    # check for empty data
    if not df or len(df) == 0:
        raise ValueError("No data returned.")
    log.info("Data is not empty")

    # validate year range
    validated_count = 0
    for row in df:
        # extract year to validate
        month_str = row[0]
        year = int(month_str.split('-')[0])  # Extract year part
        
        if year < 2014 or year > 2016:
            raise ValueError(f"Data outside expected range! Year: {year}")
        
        validated_count += 1
    
    log.info(f"Validated {validated_count} records - all years between 2014-2016")
    return "Validation succeeded"
