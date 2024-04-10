regions = [
    "Washington",
    "Virginia",
    "California",
    "Karnataka",
    "New York",
    "Texas",
    "Tennessee",
    "Telangana",
    "Massachusetts",
    "Luxembourg",
    "Arizona",
    "Tamil Nadu",
    "Ohio",
    "Mexico City",
    "Berlin",
    "Illinois",
    "Bavaria",
    "Georgia",
    "Maharashtra",
    "New South Wales",
    "Sao Paulo",
    "North-Rhine-Westphalia",
    "Colorado",
    "Ontario",
    "Community of Madrid",
    "Ile-de-France",
    "Catalonia",
    "Oregon",
    "New Jersey",
    "Lower-Saxony",
    "Maryland",
    "Florida",
    "Pennsylvania",
    "Haryana",
    "Hesse",
    "British Columbia",
    "Victoria",
    "Bratislavský kraj",
    "Lombardy",
    "National Capital Region",
    "Minnesota",
    "San Jose",
    "Western Cape",
    "Jalisco",
    "Michigan",
    "District of Columbia",
    "Indiana",
    "Istanbul",
    "Tel Aviv",
    "Heredia",
    "Saxony",
    "Quebec",
    "Piedmont",
    "Louisiana",
    "North Holland",
    "Rhineland-Palatinate",
    "Uttar Pradesh",
    "Kentucky",
    "Baden-Wurttemberg",
    "Pomeranian Voivodeship",
    "Thuringia",
    "North Carolina",
    "Delhi",
    "Auvergne-Rhone-Alpes",
    "Prague",
    "Haifa",
    "Mississippi",
    "Silesian Voivodeship",
    "Berkshire",
    "Nuevo Leon",
    "Brandenburg",
    "EDOMEX",
    "Emilia-Romagna",
    "Gujarat",
    "Queensland",
    "Sachsen-Anhalt",
    "Wisconsin",
    "Auckland",
    "Hamburg",
    "Veneto",
    "West Bengal",
    "Hauts-de-France",
    "Lubusz Voivodeship",
    "Aragon",
    "Nevada",
    "Missouri",
    "Hong Kong",
    "Alberta",
    "Bremen",
    "Centre-Val de Loire",
    "Guanajuato",
    "Latium",
    "Wellington",
    "Woj. Mazowieckie",
    "Gauteng",
    "Grand Est",
    "New Mexico",
    "Bourgogne-Franche-Comte",
    "Connecticut",
    "Styria",
    "Zurich",
    "Brussels",
    "Delaware",
    "Kansas",
    "Mecklenburg-Vorpommern",
    "Queretaro",
    "Iowa",
    "Lower Silesian Voivodeship",
    "Principality of Asturias",
    "Rajasthan",
    "South Carolina",
    "Stockholm County",
    "Středočeský kraj",
    "Wilayah Persekutuan Kuala Lumpur",
    "Central Visayas",
    "Lodz Voivodeship",
    "Nairobi",
    "Oklahoma",
    "Schleswig-Holstein",
    "South Dakota",
    "Utah",
    "Castille-La Mancha",
    "Nebraska",
    "North Dakota",
    "Olomoucký kraj",
    "Sodermanland County",
    "Vastmanland County",
    "Arkansas",
    "Flanders",
    "Provence-Alpes-Cote d'Azur",
    "Punjab",
    "Region Santiago Metropolitan",
    "South Holland",
    "Western Australia",
    "Abruzzo",
    "Buenos Aires Autonomous City",
    "Murcia Region",
    "New Hampshire",
    "Nouvelle-Aquitaine",
    "Puebla",
    "Yucatan",
    "Ústecký kraj",
    "Alabama",
    "Andhra Pradesh",
    "Loire Region",
    "Marches",
    "Tuscany",
    "Valencian Community",
    "West Pomeranian Voivodeship",
    "Brittany",
    "Coahuila",
    "Friuli-Venezia Giulia",
    "Greater Poland Voivodeship",
    "Madhya Pradesh",
    "Normandy",
    "Occitanie",
    "Quintana Roo",
    "Sonora",
    "Vienna",
    "Andalucia",
    "Assam",
    "Australian Capital Territory",
    "Baja California",
    "Capital",
    "Lower Austria",
    "Rio de Janeiro",
    "Saarland",
    "Sinaloa",
    "West and Inner Finland",
    "Bihar",
    "Campania",
    "Carinthia",
    "Castilla and Leon",
    "Galicia",
    "Geneva",
    "Hawaii",
    "Kerala",
    "Limburg",
    "Manitoba",
    "San Luis Potosi",
    "Trentino-Alto Adige",
    "Trnavský kraj",
    "Amazonas",
    "Apulia",
    "Attiki",
    "Callao",
    "Ceara",
    "Chihuahua",
    "Distrito Federal",
    "Idaho",
    "Lesser Poland Voivodeship",
    "Mexico",
    "Minas Gerais",
    "Morelos",
    "Nova Scotia",
    "Oslo",
    "Para",
    "Pernambuco",
    "Puerto Rico",
    "Rabat - Sale - Kenitra",
    "Rhode Island",
    "Sardinia",
    "South Australia",
    "South Finland",
    "Tabasco",
    "West Virginia",
]


import json
import time
import asyncio

from geopy.adapters import AioHTTPAdapter
from geopy.extra.rate_limiter import AsyncRateLimiter
from geopy.geocoders import Nominatim


async def get_locations(locations, **kwargs):
    geolocator = Nominatim(
        user_agent="ireallyjustwantafewlocations",
        adapter_factory=AioHTTPAdapter
    )
    async with geolocator as geo:
        kwarg_dict = dict.fromkeys(locations)
        kwarg_dict = {k: kwargs.copy() for k in kwarg_dict.keys()}
        kwarg_dict["Capital"].update({'country_codes': 'DK'})
        kwarg_dict["National Capital Region"].update({'country_codes': 'PH'})
        kwarg_dict["South Holland"].update({'country_codes': ['NL', 'DK']})

        geocode = AsyncRateLimiter(geo.geocode, min_delay_seconds=2)
        locs = (await asyncio.gather(*(geocode(loc, **kwarg_dict[loc]) for loc in locations)))
    return locs

kwargs = {'timeout': 7, 'extratags': True, 'language': 'en', 'featuretype': 'state', 'namedetails': True}

locs = asyncio.run(get_locations(regions, **kwargs))

addresses = [loc.address for loc in locs if loc]
zipped = dict(zip(regions, addresses))
print('almost_done')

with open('myjson.json', 'w') as f:
    js = json.dumps(zipped, indent=4)
    f.write(js)
import dill as pickle
mypickle = pickle.dumps(locs)
with open('mypickle.pkl', 'wb') as f:
    f.write(mypickle)

print("placeholder")
print("another placeholder")
import re
re.search()


matched = []
unmatched = []
for reg in regions:
    conflict = False
    poss = []
    for loc in locs:
        if reg == "California" and "Baja" in loc.address:
            continue
        if reg in " ".join([loc.raw["namedetails"].values()]):
            poss.append((reg, loc))
    if not poss:
        if match := find_match(reg, locs):
            if len(match) == 1:
                poss.append((reg, match[2]))
            else:
                conflicts.append(match)
                conflict = True
    if len(poss) == 1:
        matched.extend(poss)
        print(f"Positive ID: {poss[0][0]} \t {poss[0][1].address}\n")
    elif len(poss) >1:
        conflicts.extend(poss)
    elif not poss and not conflict:
        unmatched.append(reg)
