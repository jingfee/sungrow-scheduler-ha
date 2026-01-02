from datetime import datetime, timedelta

def get_forecast(app):
    THRESHOLD_MID = 0.15
    THRESHOLD_HIGH = 0.2
    THRESHOLD_LOW = 0.1

    data = app.get_state("sensor.power_production_next_24hours", attribute="power")

    started = False
    start_time = None
    end_time = None

    for entry in data:
        power = entry["value"]
        time = datetime.fromisoformat(entry["time"])

        if not started and power >= THRESHOLD_MID:
            if power >= THRESHOLD_HIGH:
                start_time = time - timedelta(minutes=30)
            else:
                start_time = time
            started = True

        elif started and power < THRESHOLD_MID:
            # If we were strongly producing previous hour, end earlier
            if power < THRESHOLD_LOW:
                end_time = time - timedelta(minutes=30)
            else:
                end_time = time
            break
    
    return start_time, end_time