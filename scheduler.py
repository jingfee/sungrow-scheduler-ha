import appdaemon.plugins.hass.hassapi as hass
from datetime import datetime, time, timedelta
import json
import math
import pytz
from pathlib import Path
from constants import (
    SEK_THRESHOLD,
    MIN_SOC,
    BATTERY_CAPACITY,
    AVG_ENERGY_HOURS,
    MAX_CHARGE_POWER,
)
from battery_commands import (
    set_start_charge,
    set_stop_charge,
    set_start_discharge,
    set_stop_discharge
)
from optimizer import select_night_plan
from forecast import get_forecast

SCHEDULE_FILE = Path("/conf/apps/sungrow/schedules.json")

class SungrowScheduler(hass.Hass):

    def initialize(self):
        self.log("Sungrow Scheduler started")
        self.tz = pytz.timezone("Europe/Stockholm")
        self.handles = {
            "charge": [],
            "discharge": [],
            "stop_discharge": []
        }
        if SCHEDULE_FILE.exists():
            with SCHEDULE_FILE.open() as f:
                schedules = json.load(f)
        else:
            schedules = {"charge": [], "discharge": []}
        self.charge_windows = schedules["charge"]
        self.discharge_schedule = schedules["discharge"]
        self.price_fetch_retries = 0

        # Restore existing plan on restart
        self.restore_and_schedule()

        # Schedule daily planning at 21:55
        self.run_daily(self.plan_next_day, time(21, 55))
        self.run_daily(self.check_no_nightly_charge, time(14, 00))

    def plan_next_day(self, run_time=None):
        skip = self.get_state("input_boolean.skip_next_battery_schedule") == "on"
        self.turn_off("input_boolean.skip_next_battery_schedule")
        if skip:
            self.log("Skipping schedule")
            return
        
        self.log("Running daily schedule planner")
        for key in ("charge", "discharge"):
            for h in self.handles.get(key, []):
                self.cancel_timer(h)
            self.handles[key].clear()
        
        self.current_soc = float(self.get_state("sensor.battery_level"))
        prices = self.get_prices()
        
        if self.is_summer():
            now = datetime.now(self.tz)
            tomorrow = now + timedelta(days=1)
            default_end = tomorrow.replace(hour=18, minute=0, second=0, microsecond=0)
            latest_end = tomorrow.replace(hour=20, minute=0, second=0, microsecond=0)
            forecast_start, forecast_end = get_forecast(self)
            candidate_end = forecast_end or default_end
            end_time = min(candidate_end, latest_end)
            #run discharge_after_solar at end_time

            self.discharge_schedule = []
            if forecast_end is not None:
                discharge_start = forecast_end + timedelta(hours=-2)
                discharge_end = forecast_end + timedelta(minutes=-1)
                discharge_obj = {"start": discharge_start, "end": discharge_end}
                self.discharge_schedule.append(discharge_obj)
                self.handles["discharge"].append(self.run_at(self.start_discharge, discharge_start, discharge_quarter=discharge_obj, unranked=True))

            schedules = {"charge": [], "discharge": self.discharge_schedule}
            with SCHEDULE_FILE.open("w") as f:
                json.dump(schedules, f)
        else:
            charge_quarters, discharge_quarters = select_night_plan(prices, self.get_avg_15min_energy(), self.current_soc)
            if self.current_soc > 40 and not discharge_quarters:
                self.log("No discharge from optimizer and above 40% SoC - using fallback price")
                discharge_quarters = self.get_fallback_discharge_quarters(prices)
            self.log(f"Number of nightly charge quarters {len(charge_quarters)}")
            self.log(f"Number of discharge quarters: {len(discharge_quarters)}")
            self.discharge_schedule = discharge_quarters
            self.set_night_charging(charge_quarters, len(discharge_quarters))
            
            for q in discharge_quarters:
                self.handles["discharge"].append(
                    self.run_at(
                        self.start_discharge,
                        datetime.fromisoformat(q["start"]),
                        discharge_quarter=q,
                        unranked=False
                    )
                )
            
            schedules = {"charge": self.charge_windows, "discharge": self.discharge_schedule}
            with SCHEDULE_FILE.open("w") as f:
                json.dump(schedules, f)
                
    def check_no_nightly_charge(self, run_time=None):
        if self.is_summer():
            return
        
        prices = self.get_prices()
        if len(prices) != 192:
            if self.price_fetch_retries >= 8:
                return
            self.price_fetch_retries += 1
            self.run_in(self.check_no_nightly_charge, 15 * 60)
            return
        self.price_fetch_retries = 0

        charge_quarters, discharge_quarters = select_night_plan(prices, self.get_avg_15min_energy(), MIN_SOC)
        if not charge_quarters:
            return
        
        for discharge in self.handles["discharge"]:
            self.cancel_timer(discharge)
        self.handles["discharge"].clear()
        
        ref_price = float(self.get_state("input_number.latest_night_charge_high_price"))
        self.discharge_schedule = [
            q for q in prices[15 * 4 : 46 * 4]
            if q["price"] >= ref_price + SEK_THRESHOLD
        ]
        
        for q in self.discharge_schedule:
            self.handles["discharge"].append(
                self.run_at(
                    self.start_discharge,
                    datetime.fromisoformat(q["start"]),
                    discharge_quarter=q,
                    unranked=False
                )
            )
        
        schedules = {"charge": [], "discharge": self.discharge_schedule}
        with SCHEDULE_FILE.open("w") as f:
            json.dump(schedules, f)
            
        self.turn_on("input_boolean.skip_next_battery_schedule")

    def get_prices(self):
        all_prices = []
    
        today = self.datetime().date()
        tomorrow = today + timedelta(days=1)
    
        for dt in [today, tomorrow]:
            result = self.call_service(
                "nordpool/get_prices_for_date",
                config_entry="01KBGCDMY25VMPA5FNMZCFKN4H",
                date=dt.isoformat(),
                areas="SE3",
                currency="SEK"
            )

            day_prices = (
                result
                .get("result", {})
                .get("response", {})
                .get("SE3")
            )

            if day_prices is None:
                self.log("SE3 prices not available yet")
                continue

            if len(day_prices) != 96:
                self.error(f"Invalid price data for {dt}: {day_prices}")
                continue
    
            all_prices.extend(day_prices)
    
        self.log(f"Retrieved {len(all_prices)} hourly prices")
        return all_prices
        
    def get_fallback_discharge_quarters(self, prices):
        ref_price = float(self.get_state("input_number.latest_night_charge_high_price"))
    
        day = prices[30 * 4 : 46 * 4]
    
        return [
            q for q in day
            if q["price"] >= ref_price + SEK_THRESHOLD
        ]
        
    def set_night_charging(self, charge_quarters, discharge_quarters):
        if charge_quarters == 0:
            return
        latest_balance_upper_str = self.get_state("input_text.latest_battery_balance_upper")
        latest_balance_upper = datetime.fromisoformat(latest_balance_upper_str)
        now = datetime.now(self.tz)
        diff_days = (now - latest_balance_upper).days
        should_balance_battery_upper = diff_days >= 7
        
        target_soc = self.get_target_soc(charge_quarters, discharge_quarters, should_balance_battery_upper)
        self.log(f"Target SoC {target_soc}")
        self.log(f"Current SoC {self.current_soc}")
        charge_amount = ((target_soc - self.current_soc)/100) * BATTERY_CAPACITY
        
        if charge_amount <= 0:
            # no need to charge
            return
        
        charging_power = math.ceil(((charge_amount / (len(charge_quarters) / 4)) * 1.1) / 100) * 100
        while len(charge_quarters) > 1:
            if charging_power < 800:
                # Remove the highest price quarter
                charge_quarters = sorted(charge_quarters, key=lambda x: x["price"], reverse=True)[1:]
                # Sort remaining quarters by time ascending
                charge_quarters = sorted(charge_quarters, key=lambda x: x["start"])
                
                # Recalculate charging power with 1.15 factor
                charging_power = math.ceil(((charge_amount / (len(charge_quarters) / 4)) * 1.15) / 100) * 100
            else:
                break
        charging_power = min(charging_power, MAX_CHARGE_POWER)
        
        if target_soc == 100:
            self.set_state(
                "input_text.latest_battery_balance_upper",
                state=now.isoformat()
            )
        
        high_price = max(charge_quarters, key=lambda x: x["price"])["price"]
        self.set_state("input_number.latest_night_charge_high_price", state=high_price)
        
        windows = []
        start = datetime.fromisoformat(charge_quarters[0]["start"])
        end = start + timedelta(minutes=15)
        
        for q in charge_quarters[1:]:
            if datetime.fromisoformat(q["start"]) == end:
                end += timedelta(minutes=15)
            else:
                windows.append((start, end))
                start = datetime.fromisoformat(q["start"])
                end = start + timedelta(minutes=15)
        
        windows.append((start, end))
        
        self.charge_windows  = [
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "target_soc": target_soc,
                "power": charging_power
            }
            for start, end in windows
        ]
        
        for charge_window in self.charge_windows:
            self.handles["charge"].append(self.run_at(self.start_charge, datetime.fromisoformat(charge_window["start"]), charge_window=charge_window))
            self.handles["charge"].append(self.run_at(self.stop_charge, datetime.fromisoformat(charge_window["end"]), charge_window=charge_window))
        
    def get_target_soc(self, charge_quarters, discharge_quarters, should_balance_battery_upper):
        target_soc = 0
        if discharge_quarters > 0:
            avg_15min_energy = self.get_avg_15min_energy()
            self.log(f"Avg 15min energy: {avg_15min_energy}")
            total_energy = avg_15min_energy * discharge_quarters
            self.log(f"Total energy: {total_energy}")
            target_soc = min(math.ceil(((BATTERY_CAPACITY * (MIN_SOC / 100) + total_energy)/BATTERY_CAPACITY) * 100), 100)
            self.log(f"Target soc: {target_soc}")
            
            if target_soc >= 100:
                target_soc = 100 if should_balance_battery_upper else 99
                
        charge_quarters_mean = sum(q["price"] for q in charge_quarters) / len(charge_quarters)
        target_soc = max((80 if charge_quarters_mean < 100 else 30), target_soc)
        
        if not self.is_winter():
            # if not winter - charge to max 80% since we will get some sun the next day as well
            target_soc = min(80, target_soc)
        
        return target_soc
    
    def get_avg_15min_energy(self):
        now = datetime.now(self.tz)
        start_time = now - timedelta(hours=AVG_ENERGY_HOURS)
    
        history = self.get_history(
            "sensor.total_consumed_energy",
            start_time=start_time,
            end_time=now
        )
    
        if not history or not history[0]:
            return 0.0
    
        samples = []
        for s in history[0]:
            try:
                energy_kwh = float(s["state"])
            except (ValueError, TypeError):
                continue
    
            ts = s["last_changed"]
            samples.append((ts, energy_kwh))
    
        if len(samples) < 2:
            return 0.0
    
        start_ts, start_kwh = samples[0]
        end_ts, end_kwh = samples[-1]
    
        total_energy_wh = (end_kwh - start_kwh) * 1000
        total_time_hours = (end_ts - start_ts).total_seconds() / 3600
    
        if total_time_hours <= 0:
            return 0.0
    
        # Average Wh per 15 minutes
        return total_energy_wh * (0.25 / total_time_hours)    

    def restore_and_schedule(self):
        now = datetime.now(self.tz)

        for charge_window in self.charge_windows:
            start = datetime.fromisoformat(charge_window["start"])
            end = datetime.fromisoformat(charge_window["end"])
            if end <= now:
                continue
            
            if start > now:
                self.handles["charge"].append(self.run_at(self.start_charge, start, charge_window=charge_window))
                self.handles["charge"].append(self.run_at(self.stop_charge, end, charge_window=charge_window))
            else:
                self.start_charge({"charge_window": charge_window})
                self.handles["charge"].append(self.run_at(self.stop_charge, end, charge_window=charge_window))
        
        for discharge_quarter in self.discharge_schedule:
            start = datetime.fromisoformat(discharge_quarter["start"])
            end = datetime.fromisoformat(discharge_quarter["end"])
            if end <= now:
                continue
            
            if start > now:
                self.handles["discharge"].append(self.run_at(self.start_discharge, start, discharge_quarter=discharge_quarter, unranked=False))
            else:
                self.start_discharge({"discharge_quarter": discharge_quarter, "unranked": False})

    def start_charge(self, kwargs):
        charge_window = kwargs["charge_window"]
    
        now = datetime.now(self.tz)
        if now < datetime.fromisoformat(charge_window["start"]) or now >= datetime.fromisoformat(charge_window["end"]):
            return
    
        self.log(
            f"START CHARGE "
            f"{charge_window['start']} - {charge_window['end']} | "
            f"Target SOC: {charge_window['target_soc']}% | "
            f"Power: {charge_window['power']}W"
        )
        
        set_start_charge(self, charge_window["target_soc"], charge_window["power"])
        
        handle = kwargs.get("__handle")
        if handle and handle in self.handles["charge"]:
            self.handles["charge"].remove(handle)

    def stop_charge(self, kwargs):
        charge_window = kwargs["charge_window"]
        
        now = datetime.now(self.tz)
        if now < datetime.fromisoformat(charge_window["end"]):
            return
    
        self.log("STOP CHARGE")
        
        current_soc = float(self.get_state("sensor.battery_level"))
        set_stop_charge(self)
        self.set_state("input_number.latest_charge_soc", state=charge_window['target_soc'] * current_soc)
        
        handle = kwargs.get("__handle")
        if handle and handle in self.handles["charge"]:
            self.handles["charge"].remove(handle)

    def start_discharge(self, kwargs):
        discharge_quarter = kwargs["discharge_quarter"]
        unranked = kwargs["unkranked"]
        
        now = datetime.now(self.tz)
        if now < datetime.fromisoformat(discharge_quarter["start"]) or now >= datetime.fromisoformat(discharge_quarter["end"]):
            return
        
        if unranked:
            set_start_discharge(self)
            self.handles["stop_discharge"].append(self.run_at(self.stop_discharge, datetime.fromisoformat(discharge_quarter["end"])))
        else:
            remaining = [q for q in self.discharge_schedule if datetime.fromisoformat(q["end"]) > now]
            remaining_sorted = sorted(remaining, key=lambda q: q["price"], reverse=True)
            rank = remaining_sorted.index(discharge_quarter)
            
            self.log(
                f"START DISCHARGE "
                f"{discharge_quarter['start']} - {discharge_quarter['end']} | "
                f"Rank: {rank}"
            )
            
            avg_15min_energy = self.get_avg_15min_energy()
            current_soc = float(self.get_state("sensor.battery_level"))
            discharge_capacity = ((current_soc - MIN_SOC) / 100) * 24320
            weighted_discharge_capacity = discharge_capacity * 1.15
            quarters = round(weighted_discharge_capacity / avg_15min_energy)
            self.log(f"Rank: {rank} Quarters: {quarters}")
            if rank > quarters:
                set_stop_discharge(self)
            else:
                set_start_discharge(self)
                self.handles["stop_discharge"].append(self.run_at(self.stop_discharge, datetime.fromisoformat(discharge_quarter["end"])))
        
        handle = kwargs.get("__handle")
        if handle and handle in self.handles["discharge"]:
            self.handles["discharge"].remove(handle)
        
    def stop_discharge(self, kwargs):
        now = datetime.now(self.tz)

        next_quarter = any(
            datetime.fromisoformat(q["start"]) <= now < datetime.fromisoformat(q["end"])
            for q in self.discharge_schedule
        )
        
        if next_quarter:
            return
    
        self.log("STOP DISCHARGE")
        
        set_stop_discharge(self)
        
        handle = kwargs.get("__handle")
        if handle and handle in self.handles["stop_discharge"]:
            self.handles["stop_discharge"].remove(handle)

    def set_discharge_after_solar(self, kwargs):
        current_soc = float(self.get_state("sensor.battery_level"))
        self.set_state("input_number.latest_charge_soc", state=current_soc)
        now = datetime.now(self.tz).isoformat()

        if current_soc == 100:
            self.set_state(
                "input_text.latest_battery_balance_upper",
                state=now
            )
        
        for key in ("charge", "discharge"):
            for h in self.handles.get(key, []):
                self.cancel_timer(h)
            self.handles[key].clear()
        
        prices = self.get_prices()
        forecast_start, forecast_end = get_forecast(self)

        if forecast_start is not None:
            discharge_end_hour = min(forecast_start.hour, 9)
            discharge_end_minute = (
                0 if forecast_start.hour > 9 else forecast_start.minute
            )
        else:
            discharge_end_hour = 9
            discharge_end_minute = 0

        start_index = now.hour * 4 + (2 if now.minute >= 30 else 0)
        end_index = (
            24 * 4
            + discharge_end_hour * 4
            + (2 if discharge_end_minute == 30 else 0)
        )

        discharge_quarters_price_sorted = sorted(
            (
                p for p in prices[start_index:end_index]
                if p["price"] >= -200
            ),
            key=lambda p: p["price"],
            reverse=True
        )
    
    def is_summer(self):
        now = datetime.now(self.tz)
    
        if now.month in (5, 6, 7, 8):
            return True
        elif now.month == 4:
            return now.day >= 10
        else:
            return False
    
    
    def is_winter(self):
        now = datetime.now(self.tz)
    
        if now.month in (11, 12, 1, 2):
            return True
        elif now.month == 10:
            return now.day >= 10
        elif now.month == 3:
            return now.day < 10
        else:
            return False
