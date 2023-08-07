﻿# thesis-toolset-backend

This is the repository for the server in thesis: "Data acquisition and preparation toolbox
for cumulocity-based solutions"

## Installing and running the server

1. Clone this repository `git clone https://github.com/argruk/thesis-toolset-backend.git`.
2. Make sure you have Python 3.7.7 or higher. (RECOMMENDED: install [virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) for Python)
3. Install necessary packages: `pip install -r requirements.txt`.
4. Add the credentials.json file with your credentials to the root folder.
6. Start the application with `uvicorn main:app`.
