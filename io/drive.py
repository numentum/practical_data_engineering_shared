import io
import os
import json
import numpy as np
import pandas as pd
from os import path
from time import time
from threading import Thread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


def create_drive_client(creds_path="./credentials.json"):
    """
    Creates a Google Drive Client that can be used by other functions to execute operations on Google Drive.

    Returns
    -------
    drive : Resource
        Google Drive Resource object.

    Raises
    ------
    FileNotFoundError
        If the specified credentials file does not exist.
    """

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    if path.isfile(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
    else:
        drive_creds = os.getenv("DRIVE_CREDS")
        assert drive_creds is not None, "DRIVE_CREDS environment variable not set"
        credentials = service_account.Credentials.from_service_account_info(json.loads(drive_creds))

    scoped_credentials = credentials.with_scopes(scopes)

    drive = build("drive", "v3", credentials=scoped_credentials)

    return drive


def list_files():
    """
    Lists all the files in the connected Google Drive account.

    Returns
    -------
    files : List
        List of files in Google Drive. Each file is represented by a dict, which contains the following keys:
            kind, id, name, mimeType
    """
    drive = create_drive_client()
    request = drive.files().list().execute()
    files = request.get("files", [])

    # Filter out folders
    files = [f for f in files if f["mimeType"] != "application/vnd.google-apps.folder"]

    if len(files) == 0:
        print("No files found. Does your service account has access to your Drive?")

    return files


def download_file(file_id):
    """
    Downloads a file from Google Drive.

    Params
    ------
    file_id: ID of the file to download as returned by the `list_files` function.

    Returns
    -------
    file_content : IO object with location.

    Raises
    ------
    HttpError
        If the file could not be downloaded.
    """

    try:
        # Get Drive client
        drive = create_drive_client()

        # Create download request
        request = drive.files().get_media(fileId=file_id)

        # Create BytesIO objet to store the results
        f = io.BytesIO()

        # Pass file and request to create downlaoder object
        downloader = MediaIoBaseDownload(f, request)

        # Download the file in chunks
        done = False
        while done is False:
            status, done = downloader.next_chunk()

    except HttpError as error:
        print(f"An error occurred while downloading file from Google Drive: {error}")
        return

    file_content = f.getvalue()

    return file_content


def load_file_to_df(f, verbose=False, context=None):
    """
    Parses filename, loads the content of the file from Drive, then adds the info contained in the filename
    to the DataFrame before it returns it.

    Params
    ------
    f : drive_file
        Dict containing info about a file on Drive, as returned by list_files function.

    verbose : bool
        Pass true to enable more logging, including execution times.

    Returns
    -------
    file_df pd.DataFrame
        Contents of the loaded file, including the info stored in the filename.
    """

    # if verbose:
    msg = f"Starting to download {f['name']}"
    if context is not None:
        context.log.info(msg)
    else:
        print(msg)

    filename, ext = f["name"].split(".")

    if filename.count("__") != 2:
        msg = f"File name '{f['name']}' is not following the expected format. Double underscore count should be 2, but it is {filename.count('__')}."
        if context is not None:
            context.log.info(msg)
        else:
            print(msg)
        raise ValueError(msg)

    location, date, employee = filename.split("__")

    start_time = time()
    file_content = download_file(f["id"])

    # if verbose:
    msg = f"File '{f['name']}' downloaded in {round(time() - start_time, 2)} seconds."
    if context is not None:
        context.log.info(msg)
    else:
        print(msg)

    file_df = pd.read_excel(io=file_content)

    # Add info stored in filename to their own columns
    file_df["location"] = location
    file_df["employee"] = employee
    file_df["date"] = date

    return file_df


def load_files_to_df(files=None, dfs_in=None, verbose=False, context=None):
    """
    Loads the passed / all files from Drive and concatenates them to a DataFrame.
    This function is intended to be passed to Threads. In that case, aggregator lists can be optionally
    passed, and the results will be appended to them. This way the processing does not depend on the
    return values of this function.

    Params
    ------
    files : drive_file[]
        Optional list of (subset of) files as returned by the list_files function.

    dfs_in : []
        Optional aggregator list where the loaded DataFrames will be appended.

    verbose : bool
        Pass true to enable more logging, including execution times.

    Returns
    -------
    concat_df : pd.DataFrame
        Concatenated DataFrame containing the content of all the loaded files.

    malformed_filenames : str[]
        List of filenames that could not be parsed.
    """

    # List files if no input was passed
    if files is None:
        files = list_files()

    # Create empty list as aggregator if no list was passed
    if dfs_in is not None:
        dfs = dfs_in
    else:
        dfs = []

    # Init list to collect malformed filenames
    malformed_filenames = []
    for f in files:
        try:
            file_df = load_file_to_df(f, verbose=verbose, context=context)
            context.log.info(f"Appending {len(file_df)} to dfs with len ({len(dfs)}))")
            dfs.append(file_df)
        except ValueError:
            # if verbose:
            msg = f"File name '{f['name']}' is not following the expected format."
            if context is not None:
                context.log.info(msg)
            else:
                print(msg)

            malformed_filenames.append(f["name"])
            continue

    if len(dfs) > 0:
        concat_df = pd.concat(dfs)
    else:
        concat_df = pd.DataFrame()

    return concat_df, malformed_filenames


def parallel_load_files_to_df(thread_count=25, verbose=False, context=None):
    """
    This function spawns parallel threads to speed up loading of files from Drive.

    Params
    ------
    thread_count : int
        Number of parallel threads to spawn.

    verbose : bool
        Pass true to enable more logging, including execution times.
    """

    # Get all files on Drive
    files = list_files()

    msg = f"Found {len(files)} files"
    if context is None:
        print(msg)
    else:
        context.log.info(msg)

    # Split the list of files to equal chunks, one for each thread
    file_chunks = np.array_split(files, thread_count)

    # Spawn threads
    start_time = time()
    dfs = []
    threads = []
    for chunk in file_chunks:
        t = Thread(target=load_files_to_df, args=(chunk, dfs, verbose, context))
        t.start()
        threads.append(t)

    # Wait for all the threads to finish
    for t in threads:
        t.join()

    msg = f"Loading dataframes from Drive in parallel took {round(time() - start_time, 2)} seconds"
    if context is None:
        print(msg)
    else:
        context.log.info(msg)

    if len(dfs) == 0:
        return pd.DataFrame()

    # Collect results
    market_df = pd.concat(dfs)

    return market_df
