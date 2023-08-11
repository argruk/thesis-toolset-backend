# thesis-toolset-backend

To have an application working, you also need to install two other projects:
1. [Data function](https://github.com/argruk/thesis-dashboard-data-function)
2. [Frontend](https://github.com/argruk/dashboard-frontend)

This project also requires a `credentials.json` file present in the root folder of this project.  
![credentialsExample](https://github.com/argruk/thesis-toolset-backend/assets/36072338/52603aab-7b37-4a51-bf63-99ceed3795dd)


This is the repository for the server in thesis: "Data acquisition and preparation toolbox
for cumulocity-based solutions"

## Installing and running the server

1. Clone this repository `git clone https://github.com/argruk/thesis-toolset-backend.git`.
2. Make sure you have Python 3.7.7 or higher. (RECOMMENDED: install [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) for Python)
3. Install necessary packages: `pip install -r requirements.txt`.
4. Add the credentials.json file with your credentials to the root folder.
6. Start the application with `uvicorn main:app`.
