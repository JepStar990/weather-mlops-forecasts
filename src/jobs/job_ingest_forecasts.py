from src.etl.ingest_open_meteo import main as om
from src.etl.ingest_met_no import main as met
from src.etl.ingest_openweather import main as ow
from src.etl.ingest_visual_crossing import main as vc
from src.etl.ingest_weather_gov import main as nws

def main():
    om()
    met()
    ow()
    vc()
    nws()

if __name__ == "__main__":
    main()
