## Extract tournament data from pro players from Leaguepedia and store it in an RDBMS

This is a project where I use Python and libraries like BeautifulSoup and Pandas for web scraping the [Leaguepedia](https://lol.fandom.com/wiki/League_of_Legends_Esports_Wiki "Leaguepedia") website, clean the data from League of Legends tournaments, and then save it in PostgreSQL for future analysis with BI tools. With this data, we can view trends over the years, tournaments from different regions, etc. All of this runs in Airflow within a Docker container for scheduling and debugging in case of an error.

## Explanation video
[![ETL LOLPEDIA](assets%2Fminiatura.png)](https://www.youtube.com/watch?v=oIsBHab1jwE)

## Overview

#### The pipeline is designed to:

1. **Extract data from the League of Legends Esports wiki.**
2. **Transform and store the data as a CSV.**
3. **Load the transformed data into a PostgreSQL database for analytics and querying.**
![pipeline_diagram.jpg](assets%2Fpipeline_diagram.jpg)
## Tools
1. **[Leaguepedia](https://lol.fandom.com/wiki/League_of_Legends_Esports_Wiki "Leaguepedia")**: Source of the data.
2. **Apache Airflow**: Orchestrates the ETL process and manages task distribution.
3. **Pandas library**: Managing and cleaning data.
4. **PostgreSQL**: Storage of the data.
5. **BeautifulSoup library**: For web scraping.
6. **Docker**: Contains the execution of Airflow.


## Parameters for the first task: 
- We can use only the URL (recommended).
- Or we can create some URLs with the parameters of the first task, but many URLs have different structures.
