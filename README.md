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
4. Connect to the Chula VPN (if you are not in the Chula network)
5. Run the docker compose command `docker-compose up --build`
6. Open a browser and go to `localhost:8080` to access the Airflow dashboard with the username and password as `airflow`
7. Run the DAGs (Download first, then Clean) in the Airflow dashboard
8. Run the streamlit app by running `streamlit run streamlit_viz_code/main.py` in the terminal
9. Open a browser and go to `localhost:8501` to access the Streamlit app