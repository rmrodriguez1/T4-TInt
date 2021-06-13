import requests
from xml.etree import ElementTree as ET
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(filename="credentials.json")
sh = gc.open_by_key("1IXcwUcgFUrTWpuTaHVewUUNEctEVUS0MewckBQuI53g")
worksheet = sh.get_worksheet(0)

url = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{}.xml"

indicadores = set([
    "Number of deaths", 
    "Number of infant deaths", 
    "Number of under-five deaths", 
    "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
    "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)", 
    "Estimates of number of homicides",
    "Crude suicide rates (per 100 000 population)",
    "Mortality rate attributed to unintentional poisoning (per 100 000 population)", 
    "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
    "Estimated road traffic death rate (per 100 000 population)",
    "Estimated number of road traffic deaths",
    "Mean BMI (kg/m&#xb2;) (crude estimate)",
    "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
    "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)",
    "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
    "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
    "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
    "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
    "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
    "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
    "Estimate of daily cigarette smoking prevalence (%)",
    "Estimate of daily tobacco smoking prevalence (%)",
    "Estimate of current cigarette smoking prevalence (%)",
    "Estimate of current tobacco smoking prevalence (%)",
    "Mean systolic blood pressure (crude estimate)",
    "Mean fasting blood glucose (mmol/l) (crude estimate)",
    "Mean Total Cholesterol (crude estimate)"
])

cols = ["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "Display", "Numeric", "Low", "High"]

all_data = []

paises = ["KEN", "IND", "NZL", "SLV", "CHE", "LAO"]
for pais in paises:
    r = requests.get(url.format(pais))
    root = ET.fromstring(r.text)

    for fact in root:

        data = {c: None for c in cols}
        flag = False

        for child in fact:
            if child.tag == "GHO":
                if child.text in indicadores:
                    flag = True
            if child.tag in cols:
                data[child.tag] = child.text
        
        if flag:
            all_data.append(data)

df = pd.DataFrame(all_data)

set_with_dataframe(worksheet, df)