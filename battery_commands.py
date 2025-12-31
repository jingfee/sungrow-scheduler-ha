def set_start_charge(app, target_soc, power):
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_ems_mode",
        option="Forced mode"
    )
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_battery_forced_charge_discharge_cmd",
        option="Forced charge"
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_max_soc",
        value=target_soc
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_forced_charge_discharge_power",
        value=power
    )

def set_stop_charge(app):
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_battery_forced_charge_discharge_cmd",
        option="Stop (default)"
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_max_soc",
        value=100
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_forced_charge_discharge_power",
        value=0
    )

def set_start_discharge(app):
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_ems_mode",
        option="Self-consumption mode (default)"
    )
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_battery_forced_charge_discharge_cmd",
        option="Stop (default)"
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_max_soc",
        value=100
    )
    app.call_service(
        "input_number/set_value",
        entity_id="input_number.set_sg_forced_charge_discharge_power",
        value=0
    )
    
def set_stop_discharge(app):
    app.call_service(
        "input_select/select_option",
        entity_id="input_select.set_sg_ems_mode",
        option="Forced mode"
    )
