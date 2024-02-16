# TECHIN 510 - Lab 5 - Seattle Events App
https://510seattleevents.azurewebsites.net/

### Overview

- scraper.py: scrape the events detail information and weather forecast from api and insert into pg
- app.py: main script for the events statistic app

## how to run
first, running the virtual environment
```bash
venv\Scripts\Activate.ps1
```
then we will use command to install the package list in reqirement
```bash
pip install -r requirements.txt
```

To run the streamliit app use the following command:

```bash
streamlit run app.py
```

### Lessions Learned
- how to read data from a database
  - using Pandas and pandas.io.sql.
- What if the table in database shows "null" but there is no error in running and insert?
  - delete the whole "event"table and rerun the scraper

### Future improvement
We can explore more insightful analyses based on the available data, potentially involving statistical or machine learning techniques.Implement robust error handling to gracefully handle unexpected situations, providing better user experience.