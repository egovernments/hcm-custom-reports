from datetime import datetime


def get_custom_dates_of_reports(start_date_str, end_date_str):
    """Parse report start/end dates ('%Y-%m-%d %H:%M:%S%z').

    Returns (lte_epoch_ms, gte_epoch_ms, start_date_str, end_date_str).
    """
    date_format = '%Y-%m-%d %H:%M:%S%z'

    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    gte_time = start_date.timestamp() * 1000
    lte_time = end_date.timestamp() * 1000
    print("Reports start date:", start_date)
    print("Reports end date:", end_date)

    return int(lte_time), int(gte_time), start_date_str, end_date_str
