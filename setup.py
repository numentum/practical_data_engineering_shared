from setuptools import find_packages, setup

setup(
    name="practical_data_engineering_shared",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pandas_datareader",
        "oauth2client",
        "google-api-python-client",
        "psycopg2-binary",
        "scipy",
        "statsmodels",
        "geopy",
        "openpyxl",
    ],
)
