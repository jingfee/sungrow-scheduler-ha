import math
from constants import (
    MIN_SOC,
    BATTERY_CAPACITY,
    MAX_CHARGE_POWER,
    SEK_THRESHOLD,
    STANDARD_DEVIATION_THRESHOLD
)

def select_night_plan(prices, avg_15min_energy_wh, current_soc):
    night_prices = sorted(
        {q["price"] for q in prices[22 * 4 : 30 * 4]}
    )

    best = None

    for price in night_prices:
        candidate = evaluate_candidate(
            prices,
            price,
            avg_15min_energy_wh,
            current_soc,
        )
        if not candidate:
            continue

        key = (
            candidate[2],        # est_discharge_quarters
            len(candidate[0]),   # charge_quarters
        )

        if best is None or key > best[0]:
            best = (key, candidate)
            
    cheap_quarters = [q for q in prices[22 * 4 : 30 * 4] if q["price"] < 100]

    if best:
        charge_quarters, discharge_quarters = best[1][0], best[1][1]
        unique_charge = {q["start"]: q for q in charge_quarters + cheap_quarters}
        charge_quarters = sorted(unique_charge.values(), key=lambda q: q["start"])
        return charge_quarters, discharge_quarters
    else:
        return sorted(cheap_quarters, key=lambda q: q["start"]), []
    

def evaluate_candidate(prices, max_charge_price, avg_15min_energy_wh, current_soc):
    if avg_15min_energy_wh <= 0:
        return None

    night = prices[22 * 4 : 30 * 4]
    day = prices[30 * 4 : 46 * 4]

    charge_quarters = [q for q in night if q["price"] <= max_charge_price]
    if not charge_quarters:
        return None
        
    standard_deviation = get_standard_deviation(charge_quarters)
    if standard_deviation > STANDARD_DEVIATION_THRESHOLD:
        return None

    discharge_quarters = [
        q for q in day
        if q["price"] >= max_charge_price + SEK_THRESHOLD
    ]
    if not discharge_quarters:
        return None

    max_charge_energy = (
        len(charge_quarters)
        * MAX_CHARGE_POWER
        * 0.25
    )

    usable_existing_energy = max(
        (current_soc / 100 - MIN_SOC) * BATTERY_CAPACITY,
        0,
    )

    remaining_capacity = (
        (1 - MIN_SOC) * BATTERY_CAPACITY
        - usable_existing_energy
    )

    chargeable_energy = min(
        max_charge_energy,
        remaining_capacity,
    )

    available_energy = usable_existing_energy + chargeable_energy

    est_discharge_quarters = min(
        int(available_energy // avg_15min_energy_wh),
        len(discharge_quarters),
    )

    if est_discharge_quarters == 0:
        return None

    return (
        sorted(charge_quarters, key=lambda q: q["start"]),
        sorted(discharge_quarters, key=lambda q: q["start"]),
        est_discharge_quarters,
    )

def get_standard_deviation(prices):
    mean = sum(q['price'] for q in prices) / len(prices)
    variance = sum((q['price'] - mean) ** 2 for q in prices) / len(prices)
    return math.sqrt(variance)
