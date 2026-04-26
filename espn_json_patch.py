import espn_extensions


_original_find_schedule_match = espn_extensions._find_schedule_match


def _strip_private_date_fields(record):
    if record is None:
        return None
    return {
        key: value
        for key, value in record.items()
        if key != "match_date_parsed"
    }


def _patched_find_schedule_match(fixture, aliases, schedule_records):
    result = _original_find_schedule_match(fixture, aliases, schedule_records)
    return _strip_private_date_fields(result)


espn_extensions._find_schedule_match = _patched_find_schedule_match
