import sys
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

engine = create_engine("postgresql://bdsp:@localhost:5432/bdsp")

now = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
rem = now.minute % 15
forecast_start = now + dt.timedelta(minutes=(15 - rem) % 15 or 15)

t_168 = forecast_start - dt.timedelta(hours=168)
t_24 = forecast_start - dt.timedelta(hours=24)

print(f"Forecast Start: {forecast_start}")
print(f"t_168: {t_168}")
print(f"t_24: {t_24}")

df_e = pd.read_sql(
    f"SELECT * FROM training_features WHERE time = '{t_168}'",
    engine
)
if df_e.empty:
    print(f"MISSING DATA for t_168 ({t_168}) in training_features!")
else:
    print(f"Data exists for t_168. lag_1h: {df_e.iloc[0].get('lag_1h')}, lag_24h: {df_e.iloc[0].get('lag_24h')}")

df_24 = pd.read_sql(
    f"SELECT * FROM training_features WHERE time = '{t_24}'",
    engine
)
if df_24.empty:
    print(f"MISSING DATA for t_24 ({t_24}) in training_features!")
else:
    print(f"Data exists for t_24. lag_1h: {df_24.iloc[0].get('lag_1h')}, lag_24h: {df_24.iloc[0].get('lag_24h')}")

