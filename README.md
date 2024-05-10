# One Pun Man
This repository contains all main files for the final project of group "One Pun Man" for the Data Science course.

## The main components of the project are:
- Scopus API data collection
- Airflow pipeline for data processing
- Redis database
- Streamlit app for data visualization

## How to run the project
1. Clone the repository
2. Install the required packages by running `pip install -r streamlit_req.txt`
3. Download given data from [this google drive link](https://drive.google.com/drive/folders/1Qndie0dRyqe6pHoJK-KiPqgGBic6wpDn?usp=drive_link), unzip it in the `raw_data` folder, and rename the `project` folder to `raw_given_data` (The `project` folder is obtained from unzipping the downloaded file.)
4. Connect to the Chula network (for downloading data from the Scopus API)
5. Run the docker compose command `docker-compose up --build`
6. Open a browser and go to `localhost:8080` to access the Airflow dashboard with the username and password as `airflow`
7. Activate and run the `download_scraped_data_dag` to download the data from the Scopus API (This step takes a long time to run because it needs to download the data from the Scopus API. Also this step requires Chula network to access the Scopus API.)
    - or you can activate and run the `download_scraped_data_from_github_dag` instead to download the same data from our GitHub which is prescraped
8. Activate and run the `clean_data_to_redis_dag` after the `download_scraped_data_dag` is finished to clean the data and store it in the Redis database
8. Run the streamlit app by running `streamlit run streamlit_viz_code/main.py` in the terminal
9. Open a browser and go to `localhost:8501` to access the Streamlit app