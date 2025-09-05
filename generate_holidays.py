"""
This script is here for documentation purposes only.
It has already been run to generate dbt_transformations/seeds
"""
import holidays
import csv

def generate_holidays(filepath: str) -> None:
    """Generate a csv file of holidays, saved at filepath"""
    years = range(2013, 2027)
    combined_holidays = holidays.financial_holidays("NYSE", years=years)
    
    # ny holidays append to nyse holidays
    ny_holidays = holidays.country_holidays("US", subdiv="NY", years=years)
    for holiday_date, holidays_for_date in ny_holidays.items():
        if holiday_date in combined_holidays:
            combined_holidays[holiday_date] = f"{combined_holidays[holiday_date]}; {holidays_for_date}"
        else:
            combined_holidays[holiday_date] = holidays_for_date
    
    with open(filepath, "w") as f:
        fieldnames = ["holiday_date", "holiday_name"]
        writer = csv.writer(f)

        writer.writerow(fieldnames)
        for holiday_date, holidays_for_date in sorted(combined_holidays.items()):
            row = [
                holiday_date.strftime("%Y-%m-%d"),
                holidays_for_date.replace("'", "\\'")
            ]
            writer.writerow(row)

if __name__ == "__main__":
    FILEPATH = "dbt_transformations/seeds/holidays.csv"
    generate_holidays(FILEPATH)