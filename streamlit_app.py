# streamlit-app.py
##
## This Streamlit (https://streamlit.io/) app replaces my old `network-file-finder` command line utility.
## Most functionality is unchanged with a few new features added.  The old command line parameters
## have been replaced with similarly named GUI widgets.
##
## The old `network-file-finder` comments...
## -----------------------------------------------------------------------------------------------------
## This script is designed to read all of filenames from a specified --column of a specified
## --worksheet Google Sheet and fuzzy match with files found in a specified --tree-path
## network (or other mounted) storage directory tree.
##
## If the --copy-to-azure option is set this script will attempt to deposit copies of any/all
## OBJ files it finds into Azure Blob Storage.  If --extended (-x) is also specified, the script will also
## search for and copy all _TN. and _JPG. files (substituting those for _OBJ.) that it finds.
## The copy-to-azure operation will also generate a .csv file containing Azure Blob URL(s) suitable
## for input into the `object_location`, `image_small`, and `image_thumb` columns of a CollectionBuilder CSV ## file or Google Sheet.
## -----------------------------------------------------------------------------------------------------

import os
import streamlit as st
import json
import gspread as gs
from gspread_dataframe import set_with_dataframe
import re
import csv
import shutil
from fuzzywuzzy import process
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
from thumbnail import generate_thumbnail
from wand.image import Image
from loguru import logger
# from streamlit.logger import get_logger
from subprocess import call

# Globals

azure_base_url = "https://dgobjects.blob.core.windows.net/"
column = 7     # Default column for filenames is 'G' = 7
skip_rows = 1  # Default number of header rows to skip = 1
levehstein_ratio = 90
significant = False
kept_file_list = False
copy_to_azure = False
extended = False
grinnell = False
use_match_list = False
counter = 0
csvlines = [ ]
big_file_list = [ ]   # need a list of just filenames...
big_path_list = [ ]   # ...and parallel list of just the paths
significant_file_list = [ ]
significant_path_list = [ ]
significant_dict = { }
sheet_url = False

# Functions defined and used in https://gist.github.com/benlansdell/44000c264d1b373c77497c0ea73f0ef2
# ---------------------------------------------------------------------

def update_dir(key):
    choice = st.session_state[key]
    if os.path.isdir(os.path.join(st.session_state[key+'curr_dir'], choice)):
        st.session_state[key+'curr_dir'] = os.path.normpath(os.path.join(st.session_state[key+'curr_dir'], choice))
        files = sorted(os.listdir(st.session_state[key+'curr_dir']))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files

def st_file_selector(st_placeholder, path, label='Select a file/folder', key='dir_selector_'):
    if key+'curr_dir' not in st.session_state:
        base_path = '.' if path is None or path == '' else path
        base_path = base_path if os.path.isdir(base_path) else os.path.dirname(base_path)
        base_path = '.' if base_path is None or base_path == '' else base_path

        files = sorted(os.listdir(base_path))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files
        st.session_state[key+'curr_dir'] = base_path
    else:
        base_path = st.session_state[key+'curr_dir']

    selected_file = st_placeholder.selectbox(label=label,
                                        options=st.session_state[key+'files'],
                                        key=key,
                                        on_change = lambda: update_dir(key))

    selected_path = os.path.normpath(os.path.join(base_path, selected_file))
    st_placeholder.write(os.path.abspath(selected_path))

    if st_placeholder.button("Submit Directory Selection", "stfs_submit_button", "Click here to confirm your directory selection"):
        return selected_path

# My functions
# ---------------------------------------------------------------------


# upload_to_azure( ) - Just what the name says post-processing
# ----------------------------------------------------------------------------------------------
def upload_to_azure(blob_service_client, url, match, local_storage_path, transcript=False):

    try:

        if transcript:
            container_name = "transcripts"
        elif "thumbs/" in url:
            container_name = "thumbs"
        elif "smalls/" in url:
            container_name = "smalls"
        else:
            container_name = "objs"

        # Create a blob client using the local file name as the name for the blob
        if container_name:
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=match)
            if blob_client.exists():
                txt = f"Blob '{match}' already exists in Azure Storage container '{container_name}'.  Skipping this upload."
                st.success(txt)
                state('logger').success(txt)
                return "EXISTS"
            else:
                txt = f"Uploading '{match}' to Azure Storage container '{container_name}'"
                st.success(txt)
                state('logger').success(txt)

                # Upload the file
                with open(file=local_storage_path, mode="rb") as data:
                    blob_client.upload_blob(data)
                return "COPIED"

        else:
            txt = f"No container available for uploading '{match}' to Azure Storage!'"
            st.error(txt)
            state('logger').error(txt)
            return False

    except Exception as ex:
        state('logger').critical(ex)
        st.exception(ex)
        pass


# check_significant(regex, filename)
# ---------------------------------------------------------------------------------------
def check_significant(regex, filename):
    import re

    if '(' in regex:             # regex already has a (group), do not add one
        pattern = regex
    else:
        pattern = f"({regex})"     # regex is raw, add a (group) pair of parenthesis

    try:
        match = re.search(pattern, filename)
        if match:
            return match.group( )
        else:
            return False
    except Exception as e:
        assert False, f"Exception: {e}"


# build_lists_and_dict(significant, target, files_list, paths_list)
# ---------------------------------------------------------------------------------------
def build_lists_and_dict(significant, target, files_list, paths_list):
    significant_file_list = []
    significant_path_list = []
    significant_match = False
    is_significant = "*"

    # If a --regex (significant) was specified see if our target has a matching component...
    if significant:
        significant_match = check_significant(significant, target)
        if significant_match:   # ...it does, pare the significant_*_list down to only significant matches
            for i, f in enumerate(files_list):
                is_significant = check_significant(significant_match, f)
            if is_significant:
                significant_file_list.append(f)
                significant_path_list.append(paths_list[i])

    # If there's no significant_match... make the output lists match the input lists
    if not significant_match:
        significant_file_list = files_list
        significant_path_list = paths_list

    # Now, per https://github.com/seatgeek/fuzzywuzzy/issues/165 build an indexed dict of significant files
    file_dict = {idx: el for idx, el in enumerate(significant_file_list)}

    # Return a tuple of significant match and the three significant lists
    return (significant_match, significant_file_list, significant_path_list, file_dict)


# open_google_sheet(sheet_url)
# --------------------------------------------------------------
def open_google_sheet(sheet_url):

    try:
        sa = gs.service_account()
    except Exception as e:
        state('logger').critical(e)
        st.exception(e)

    try:
        sh = sa.open_by_url(sheet_url)
    except Exception as e:
        state('logger').critical(e)
        st.exception(e)

    return sh


# open_google_worksheet(sheet_url, worksheet_title)
# --------------------------------------------------------------
def open_google_worksheet(sheet_url, worksheet_title):

    sh = open_google_sheet(sheet_url)

    # Open the specified worksheet (tab) and return it
    worksheet = sh.worksheet(worksheet_title)
    return worksheet


# fuzzy-search-for-files(status)
# All parameters come from st.session_state...
# --------------------------------------------------------------------------------------
def fuzzy_search_for_files(status):

    # Get st.session_state parameters
    kept_file_list = state('use_previous_file_list')
    sheet_url = state('google_sheet_url')
    worksheet_title = state('google_worksheet_selection')
    column = state('worksheet_column_number')
    path = state('stfs_path_selection')
    regex = state('regex_text')

    csvlines =  [ ]
    counter = 0
    filenames = [ ]

    # Check the --kept-file-list switch.  If it is True then attempt to open the `file-list.tmp` file
    # saved from a previous run.  The intent is to cut-down on Google API calls.
    if kept_file_list:
        try:
            with open('file-list.tmp', 'r') as file_list:
                for filename in file_list:
                    if filename:
                        filenames.append(filename.strip())
                    else:
                        filenames.append("")

        except Exception as e:
            kept_file_list = False
            pass

        # If processing_mode is selected, issue an apology... can't do that
        # unless a Google Sheet is specified.

        if state('processing_mode'):
            txt = f"Sorry, we can't write to your Google Sheet if you don't select one for processing."
            st.warning(txt)
            state('logger').warning(txt)

    # If we aren't using a kept file list... Open the Google service account and sheet
    else:

        worksheet = open_google_worksheet(sheet_url, worksheet_title)

        # Grab all filenames from --column
        filenames = worksheet.col_values(column)

        # Save the filename list in 'file-list.tmp' for later
        try:
            with open('file-list.tmp', 'w') as file_list:
                for filename in filenames:
                    file_list.write(f"{filename}\n")
        except Exception as e:
            state('logger').critical(e)
            st.exception(e)
            exit( )

        # If processing_mode is selected, copy the contents of the Google Sheet into a dataframe
        # so we can post updates/additions to the sheet without calling the Google API too many times.

        if state('processing_mode'):
            data = worksheet.get_all_values( )
            headers = data.pop(0)
            st.session_state['df'] = pd.DataFrame(data, columns=headers)

    # Grab all non-hidden filenames from the target directory tree so we only have to get the list once
    # Exclusion of dot files per https://stackoverflow.com/questions/13454164/os-walk-without-hidden-folders

    for root, dirs, files in os.walk(path):
        files = [f for f in files if not f[0] == '.']
        dirs[:] = [d for d in dirs if not d[0] == '.']
        for filename in files:
            big_path_list.append(root)
            big_file_list.append(filename)

    # Check for ZERO network files in the big_file_list
    if len(big_file_list) == 0:
        txt = f"The specified --tree-path of '{path}' returned NO files!  Check your path specification and network connection!\n"
        st.error(txt)
        state('logger').error(txt)
        exit()

    # # Report our --regex option...
    # if significant:
    #   my_colorama.green(f"\nProcessing only files matching signifcant --regex of '{significant}'!")
    # else:
    #   my_colorama.green(f"\nNo --regex specified, matching will consider ALL paths and files.")

    progress_text = "Fuzzy search in progress.  Be patient."
    search_progress = st.progress(0, progress_text)

    # Now the main matching loop...
    num_filenames = len(filenames)

    for x in range(num_filenames):

        percent_complete = min(x / num_filenames, 100)
        search_progress.progress(percent_complete, progress_text)

        if x < skip_rows:  # skip this row if instructed to do so
            txt = f"Skipping match for '{filenames[x]}' in worksheet row {x}"
            st.warning(txt)
            state('logger').warning(txt)
            continue  # move on and process the next row

        if len(filenames[x]) < 1:  # filename is empty, skip this row 
            txt = f"Skipping match for BLANK filename in worksheet row {x}"
            st.warning(txt)
            state('logger').warning(txt)
            continue  # move on and process the next row

        counter += 1
        target = filenames[x]

        # # If --grinnell is specified and the 'target' begins with 'grinnell_' AND does not contain '_OBJ'... make it so
        # if grinnell and ('grinnell_' in target) and ('_OBJ' not in target):
        #     target += '_OBJ.'

        status.update(
            label=
            f"{counter}. Finding best fuzzy filename matches for '{target}'...",
            expanded=True,
            state="running")

        # st.write(f"{counter}. Finding best fuzzy filename matches for '{target}'...")

        csv_line = [None] * 7
        significant_text = ''

        csv_line[0] = x             # was counter, but that does not account for skipped filenames!
        csv_line[1] = target
        csv_line[2] = None            # Hold our regex expression...later

        (significant_text, significant_file_list, significant_path_list,
         significant_dict) = build_lists_and_dict(significant, target,
                                                  big_file_list, big_path_list)

        # if significant_text:
        #      st.status(f"  Significant string is: '{significant_text}'.")
        #      report = significant_text

        # If target is blank, skip the search and set matches = False
        matches = False
        if len(target) > 0:
            matches = process.extract(target, significant_dict, limit=3)

        # Report the top three matches
        if matches:
            for found, (match, score, index) in enumerate(matches):
                path = significant_path_list[index]

                if found == 0:
                    csv_line[3] = score
                    csv_line[4] = match
                    csv_line[5] = path
            
                    if score == 100:
                        txt = f"!!! Found a 100 matching file: {format(csv_line)}"
                        st.success(txt)
                        state('logger').success(txt)

                    elif score > 89:
                        txt = f"!!! Found BEST but NOT 100 matching file: {format(csv_line)}"
                        st.warning(txt)
                        state('logger').success(txt)

                    else:
                        txt = f"!!! Found BEST matching file but with a poor score: {format(csv_line)}"
                        st.error(txt)
                        state('logger').warning(txt)

                # Transcript processing, if enabled... look for a .csv, .vtt, .pdf or .xml file
                if state('transfer_transcripts') and (score > 89):
                    (root, extension) = os.path.splitext(match)
                    if extension.lower( ) in ['.csv', '.vtt', '.pdf', '.xml']:
                        txt = f"!!! Transcript processing is ON and this was found: {format(csv_line)}"
                        st.success(txt)
                        state('logger').success(txt)

                        # Save the transcript filename to csv_line[ ] element 6
                        csv_line[6] = match
                    
        else:
            txt = f"*** Found NO match for: {format(' | '.join(csv_line))}"
            st.error(txt)
            state('logger').error(txt)

        # Save this fuzzy search result in 'csvlines' for return
        csvlines.append(csv_line)

        # If --output-csv is true, open a .csv file to receive the matching filenames and add a heading
        if state('output_to_csv'):
            with open('match-list.csv', 'w', newline='') as csvfile:
                list_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

                if state('significant'):
                    significant_header = f"\'{state('significant')}\' Match"
                else:
                    significant_header = "Undefined"

                header = [
                    'No.', 'Target', 'Significant --regex', 'Best Match Score',
                    'Best Match', 'Best Match Path', '2nd Match Score',
                    '2nd Match', '2nd Match Path', '3rd Match Score',
                    '3rd Match', '3rd Match Path'
                ]
                list_writer.writerow(header)

                for line in csvlines:
                    list_writer.writerow(line)

    txt = f"**Fuzzy search output is saved in 'match-list.csv**"
    st.success(txt)
    state('logger').success(txt)

    status.update(label=f"Fuzzy search is **complete**!",
                  expanded=True,
                  state="complete")

    return csvlines


# n2a(n) - Convert spreadsheet column position (n) to a letter designation per
# https://stackoverflow.com/questions/23861680/convert-spreadsheet-number-to-column-letter
# -------------------------------------------------------------------------------
def n2a(n):
    d, m = divmod(n,26) # 26 is the number of ASCII letters
    return '' if n < 0 else n2a(d-1)+chr(m+65) # chr(65) = 'A'


# state(key) - Return the value of st.session_state[key] or False
# If state is set and equal to "None", return False.
# -------------------------------------------------------------------------------
def state(key):
    try:
        if st.session_state[key]:
            if st.session_state[key] == "None":
                return False
            return st.session_state[key]
        else:
            return False
    except Exception as e:
        # st.exception(f"Exception: {e}")
        return False


# transform_list_to_dict(worksheet_list)
# ---------------------------------------------------------------------
def transform_list_to_dict(wks_dict, worksheet_list):
    for w in worksheet_list:
        parts = re.split('\'|:', str(w))
        if len(parts) > 3:
            wks_dict[parts[1]] = parts[3].rstrip('>')
        else: 
            logger.error(f"Not enough 'parts' in {parts}!")
            return False    
    return wks_dict


# get_tree( )
# ---------------------------------------------------------------------
def get_tree( ):

    # Read 'paths.json' file
    with open('paths.json', 'r') as j:
        paths = json.load(j)

    # Cannot wrap this in a form because st_file_selector( ) has a callback function
    with st.container(border=True):
        selected_root = st.selectbox('Choose a mounted root directory to navigate from', paths.keys( ), index=None, key='root_directory_selectbox')
        st.session_state.root_directory_selection = selected_root

        if state('root_directory_selection'):
            root = paths[state('root_directory_selection')]
            txt = f"Selected root directory: **\'{state('root_directory_selection')}\' with a path of \'{root}\'**"
            st.success(txt)
            state('logger').success(txt)


            st_file_selector(st, path=root, label="Select a directory root to search for the worksheet's list of files")
            st.session_state.stfs_path_selection = state('dir_selector_curr_dir')

            if state("stfs_path_selection"):
                txt = f"Selected folder path: **\'{state('stfs_path_selection')}\'**"
                st.success(txt)
                state('logger').success(txt)


    return


# get_worksheet_column_selection( )
# ----------------------------------------------------------------------
def get_worksheet_column_selection( ):

    # Wrap all the worksheet column selection in a form...
    with st.form('worksheet_form'):

        # Read 'sheets.json' file
        with open('sheets.json', 'r') as j:
            sheets = json.load(j)


        selected_google_sheet = st.selectbox('Choose a Google Sheet to work with', sheets.keys( ), index=None, key='google_sheet_selectbox')
        st.session_state.google_sheet_selection = selected_google_sheet

        if state("google_sheet_selection"):
            sheet_url = sheets[state("google_sheet_selection")]
            st.session_state.google_sheet_url = sheet_url
            txt = f"Selected Google Sheet: \'{state('google_sheet_selection')}\' with a URL of \'{sheet_url}\'"
            st.success(txt)
            state('logger').success(txt)

            selected_worksheet = state("google_worksheet_selection")
            sh = open_google_sheet(sheet_url)

            # Fetch list of worksheets and build a name:gid dict
            worksheet_list = sh.worksheets( )
            worksheet_dict = { }
            worksheet_dict = transform_list_to_dict(worksheet_dict, worksheet_list)

            # Select the worksheet to be processed
            selected_worksheet = st.selectbox('Choose the worksheet you wish to work with', worksheet_dict.keys( ), index=None, key='worksheet_selectbox')
            st.session_state.google_worksheet_selection = selected_worksheet

            if state("google_worksheet_selection"):
                txt = f"Selected worksheet: '{selected_worksheet}' with gid={worksheet_dict[selected_worksheet]}"
                st.success(txt)
                state('logger').success(txt)

                # Open the selected worksheet
                worksheet = sh.worksheet(state("google_worksheet_selection"))
                st.session_state['worksheet'] = worksheet

                # Now fetch a list of columns from the selected sheet
                column_list = worksheet.row_values(1)

                # # If `check_worksheet_column_headings` is set... check that each value from `column_list` is in our approved set of headings.  ONLY works for Alma migration at this point!
                # errors = 0
                # if state('check_worksheet_column_headings'):
                #     if 'Alma-D' not in state('google_sheet_selection'):
                #         msg = f"Checking worksheet column headings ONLY works for 'Migration-to-Alma-D' at this point!"
                #         errors = 1
                #         st.error(msg)
                    
                #     else:
                #         for h in column_list:
                #             if h not in correct_Alma_CSV_headings:
                #                 errors += 1
                #                 msg = f"Whoa! Worksheet column heading `{h}` is NOT in our approved list of headings!"
                #                 logger.critical(msg)
                #                 st.error(msg)
                #                 msg = f"You should stop this app and fix your worksheet NOW!"
                #                 st.error(msg)

                #     if errors == 0:
                #         msg = f"The worksheet column headings showed NO ERRORS.  You are good to go!"
                #         st.success(msg)

                # Make your column selection
                selected_column = st.selectbox('Choose the column containing your filenames', column_list, index=None, key='column_selector')
                st.session_state.worksheet_column_selection = selected_column

                if state('worksheet_column_selection'):
                    position = column_list.index(selected_column)
                    st.session_state['worksheet_column_number'] = position + 1   # column 'A'=1, not zero
                    col_letter = n2a(position)
                    txt = f"Selected column: \'{state('worksheet_column_selection')}\' with designation \'{col_letter}\'"
                    st.success(txt)
                    state('logger').success(txt)

        st.form_submit_button("Submit Worksheet Selection")

    return


# get_network_path(path, fname)
# ----------------------------------------------------------------------
def get_network_path(path, fname):
    return os.path.join(path, fname)


# check_numeric_part(score, target, candidate)
# ----------------------------------------------------------------
def check_numeric_part(score, target, candidate):
    pattern = re.compile(r'^.*[-_](\d+).*$')   # any ... dash OR underscore ... series of digits ... any
    m_target = pattern.match(target)
    if m_target:
        tn = m_target.group(1)
        m_candidate = pattern.match(candidate)
        if m_candidate:
            cn = m_candidate.group(1)
            if tn == cn:                 # an EXACT numeric match!
                return 95
    return score


# build_azure_url( )
# ----------------------------------------------------------------------
def build_azure_url(target, score, match, mode='OBJ'):

    # Special logic... if the score > 49 check the embedded numeric portion ONLY and
    # if that's an EXACT match we will accept it as a match
    if score > 49:
        score = check_numeric_part(score, target, match)

    try:

        # Check if the match score was 90 or above, if not, skip it!
        if score < 90:
            txt = f"Best match for '{target}' has an insufficient match score of {score}.  It will NOT be accepted nor copied to Azure storage."
            st.warning(txt)
            state('logger').warning(txt)

            return False

        # Check for obvious mode/match errors
        if "_TN." in match and mode != 'TN':
            txt = f"_TN in '{match}' and mode '{mode}' is an error!"
            st.error(txt)
            state('logger').error(txt)

            return False
        elif "_JPG." in match and mode != 'JPG':
            txt = f"_JPG in '{match}' and mode '{mode}' is an error!"
            st.error(txt)
            state('logger').error(txt)

            return False
        elif "_OBJ." in match and mode != 'OBJ':
            txt = "_OBJ in '{match}' and mode '{mode}' is an error!"
            st.error(txt)
            state('logger').error(txt)
            return False

        # Determine the type of URL to build... OBJ, TN, JPG or TRANSCRIPT
        if mode == 'TRANSCRIPT':
            url = azure_base_url + "transcripts/" + match
        elif "_TN." in match or mode == 'TN':
            url = azure_base_url + "thumbs/" + match
        elif "_JPG." in match or mode == 'JPG':
            url = azure_base_url + "smalls/" + match
        elif "_OBJ." in match or mode == 'OBJ':
            url = azure_base_url + "objs/" + match
        else:
            txt = f"'{match}' and mode '{mode}' is an error!"
            st.error(txt)
            state('logger').error(txt)
            return False

        return url

    except Exception as ex:
        state('logger').critical(ex)
        st.exception(ex)
        pass


# post_processing(status, csv_results, df)
#
# If --copy-to-azure is true... for each '_OBJ.' (and if --extended '_TN.' or '_JPG.') match
# execute a copy to Azure Blob Storage operation.  For this to work our AZURE_STORAGE_CONNECTION_STRING
# environment variable must be in place and accurate.
#
# ----------------------------------------------------------------------------
def post_processing(csv_results):

    with st.status(f"Beginning post-processing for {len(csv_results)} objects.", expanded=True, state="running") as status:

        st.session_state['copied'] = 0
        st.session_state['exists'] = 0
        st.session_state['skipped'] = 0

        try:

            # Retrieve the connection string for use with the application. The storage
            # connection string is stored in an environment variable on the machine
            # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment
            # variable is created after the application is launched in a console or with
            # Visual Studio, the shell or application needs to be closed and reloaded to take the
            # environment variable into account.

            connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

            # Create the BlobServiceClient object
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)

            # Loop on all the "matches"
            progress_text = "Post processing in progress.  Be patient."
            post_progress = st.progress(0, progress_text)

            num_matches = len(csv_results)

            for i, line in enumerate(csv_results):
                percent_complete = min(i / num_matches, 100)
                post_progress.progress(percent_complete, progress_text)
                # print(line)
                index = int(line[0])
                target = line[1]
                regex = line[2]
                score = int(line[3])
                match = line[4]
                path = line[5]
                transcript = line[6]

                # Build a network file path for the best match
                local_storage_path = get_network_path(path, match)

                # Call our file_handler for the main object
                result = file_handler(index, blob_service_client, target, score, match, local_storage_path, False)

                # If we have a transcript, call the file_handler again
                if result and transcript:
                    file_handler(index, blob_service_client, target, score, match, local_storage_path, transcript)

            # Done!
            status.update(label=f"Azure post processing is complete!", expanded=True, state="complete")

        except Exception as ex:
            state('logger').critical(ex)
            st.exception(ex)

    # Declare success!
    txt = f"Azure processing results: copied={state('copied')} exists={state('exists')} skipped={state('skipped')}"
    st.success(txt)
    state('logger').success(txt)


    # If we have an open dataframe, write it back into the Google sheet
    if isinstance(st.session_state.df, pd.DataFrame):

        # If the "Dump dataframe before save" is set, print the dataframe
        if st.session_state.dump_dataframe:
            st.dataframe(st.session_state.df)

        try:

            worksheet = open_google_worksheet(
                state('google_sheet_url'), state('google_worksheet_selection'))
            if worksheet:
                set_with_dataframe(worksheet, st.session_state.df, 1, 1)
                txt = f"Updated file URLs have been saved to the selected Google worksheet."
                st.success(txt)
                state('logger').success(txt)

            else:
                txt = f"Google worksheet at {state('google_sheet_url')} and {state('google_worksheet_selection')} could not be re-opened."
                st.error(txt)
                state('logger').error(txt)

        except Exception as ex:
            txt = f"Google worksheet at {state('google_sheet_url')} and {state('google_worksheet_selection')} was NOT updated."
            st.error(txt)
            state('logger').error(txt)

            st.write(f"Dumping the updated worksheet DataFrame...")
            st.dataframe(st.session_state.df)
            state('logger').critical(ex)
            st.exception(ex)

    else:
        txt = f"Google worksheet at {state('google_sheet_url')} and {state('google_worksheet_selection')} was NOT updated"
        st.error(txt)
        state('logger').error(txt)


# file_handler(index, blob_service_client, target, score, match, local_storage_path, transcript=False)
# ---------------------------------------------------------------------------------------
def file_handler(index, blob_service_client, target, score, match, local_storage_path, transcript=False):
    
    url = None

    # Build an Azure Blob URL for the object
    if transcript:
        url = build_azure_url(target, score, transcript, mode="TRANSCRIPT")
        match = transcript
    else:
        url = build_azure_url(target, score, match)
        if not url:
            st.session_state['skipped'] += 1

    result = False

    # Upload the file to Azure Blob storage
    if url and state('azure_blob_storage'):
        result = upload_to_azure(blob_service_client, url, match, local_storage_path, transcript)
        if not transcript:
            if result == "EXISTS":
                st.session_state['exists'] += 1
            elif result == "COPIED":
                st.session_state['copied'] += 1

    # If result is NOT False and processing_mode is targeted, put the found filename into the worksheet dataframe
    col = False
    if result and state('processing_mode'):
        if state('processing_mode') == 'CollectionBuilder':  # CollectionBuilder
            if transcript:     
                col = 'object_transcript'
            else:
                col = 'object_location'
                # col = 'WORKSPACE1'
        # elif state('processing_mode') == 'Migration to Alma':  # Alma migration
        #     col = 'file_name_1'
        else:
            txt = f"Sorry, the 'processing_mode' state of \'{st.session_state['processing_mode']}\' is not recognized"
            st.error(txt)
            state('logger').error(txt)
            return False

    if col and isinstance(st.session_state.df, pd.DataFrame):
        row = st.session_state.df.index[index - 1]  # adjust for header row!
        st.session_state.df.at[row, col] = url

        # And if this is a transcript, set the 'display_template' value to 'transcript'
        if transcript:
            st.session_state.df.at[row, 'display_template'] = 'transcript'

        # Temporary... print the dataframe
        # if isinstance(st.session_state['df'], pd.DataFrame):
        #     st.dataframe(st.session_state['df'])

    # Thumbnail creation
    if url and state('generate_thumb') and not state('copy_to_objs'):
        result = create_derivative('thumbnail', index, url, local_storage_path, blob_service_client)

    # "Small" creation
    if url and state('generate_small'):
        result = create_derivative('small', index, url, local_storage_path, blob_service_client)

    return True


# # create_clientThumb(local_storage_path)
# # ------------------------------------------------------------
# def create_clientThumb(local_storage_path):

#     dirname, basename = os.path.split(local_storage_path)
#     root, ext = os.path.splitext(basename)

#     # Create clientThumb thumbnails for Alma
#     derivative_path = f"/Volumes/exports/OBJs/{root}X.jpg"
#     dest = f"/Volumes/exports/OBJs/{root}{ext}.clientThumb"

#     options = { 'trim': False,
#                 'height': 400,
#                 'width': 400,
#                 'quality': 85,
#                 'type': 'thumbnail'
#               }

#     # If original is an image...
#     if ext.lower( ) in ['.tiff', '.tif', '.jpg', '.jpeg', '.png']:
#         generate_thumbnail(local_storage_path, derivative_path, options)
#         os.rename(derivative_path, dest)

#     # If original is a PDF...
#     elif ext.lower( ) == '.pdf':
#         cmd = 'magick ' + local_storage_path + '[0] ' + derivative_path
#         call(cmd, shell=True)
#         os.rename(derivative_path, dest)

#     else:
#         txt = f"Sorry, we can't create a thumbnail for '{local_storage_path}'"
#         st.warning(txt)
#         state('logger').warning(txt)

# create_derivative(derivative_type, index, url, local_storage_path, blob_service_client)
# ------------------------------------------------------------
def create_derivative(derivative_type, index, url, local_storage_path, blob_service_client):

    derivative_filename = None

    dirname, basename = os.path.split(local_storage_path)
    root, ext = os.path.splitext(basename)

    # # Create clientThumb thumbnails for Alma
    # if state('processing_mode') == 'Migration to Alma':
    #     derivative_path = f"/Volumes/exports/OBJs"

    #     # If original is an image...
    #     if ext.lower( ) in ['.tiff', '.tif', '.jpg', '.jpeg', '.png']:
    #         generate_thumbnail(local_storage_path, derivative_path, options)

    #     # If original is a PDF...
    #     elif ext.lower( ) == '.pdf':
    #         cmd = 'magick ' + local_storage_path + '[0] ' + derivative_path
    #         call(cmd, shell=True)

    #     else:
    #         txt = f"Sorry, we can't create a thumbnail for '{local_storage_path}'"
    #         st.warning(txt)
    #         state('logger').warning(txt)

    # If creating derivative(s) for CollectionBuilder...
    if state('processing_mode') == 'CollectionBuilder':
        
        if derivative_type == 'thumbnail':
            col = 'image_thumb'
            # col = 'WORKSPACE3'
            options = {
                'trim': False,
                'height': 400,
                'width': 400,
                'quality': 85,
                'type': 'thumbnail'
            }
            derivative_url = url.replace('/objs/', '/thumbs/').replace(ext, '_TN.jpg')
            derivative_filename = f"{root}_TN.jpg"

        elif derivative_type == 'small':
            if state('processing_mode') != 'CollectionBuilder':
                txt = f"Call to create_derivative( ) with option other than CollectionBuilder is not necessary!"
                st.error(txt)
                state('logger').error(txt)
                return False

            col = 'image_small'
            # col = 'WORKSPACE2'
            options = {
                'trim': False,
                'height': 800,
                'width': 800,
                'quality': 85,
                'type': 'thumbnail'
            }
            derivative_url = url.replace('/objs/', '/smalls/').replace(ext, '_SMALL.jpg')
            derivative_filename = f"{root}_SMALL.jpg"

        else:
            txt = f"Call to create_derivative( ) has an unknown 'derivative_type' of '{derivative_type}'."
            st.error(txt)
            state('logger').error(txt)

        derivative_path = f"/tmp/{derivative_filename}"

        # If original is an image...
        if ext.lower( ) in ['.tiff', '.tif', '.jpg', '.jpeg', '.png']:
            generate_thumbnail(local_storage_path, derivative_path, options)

        # If original is a PDF...
        elif ext.lower( ) == '.pdf':
            cmd = 'magick ' + local_storage_path + '[0] ' + derivative_path
            call(cmd, shell=True)

        else:
            txt = f"Sorry, we can't create a thumbnail for '{local_storage_path}'"
            st.warning(txt)
            state('logger').warning(txt)

            derivative_url = False

        # Upload the file to Azure Blob storage
        if derivative_url and state('azure_blob_storage'):
            result = upload_to_azure(blob_service_client, derivative_url, derivative_filename, derivative_path)

        # Save it to the dataframe
        if derivative_url and col and isinstance(st.session_state['df'], pd.DataFrame):
            df = st.session_state['df']
            row = df.index[index - 1]  # adjust for header row!
            df.at[row, col] = derivative_url

# ----------------------------------------------------------------------
# --- Main

if __name__ == '__main__':

    # Initialize the session_state
    if not state('logger'):
        logger.add("app.log", rotation="500 MB")
        logger.info('This is streamlit_app.py!')
        st.session_state.logger = logger
    if not state('root_directory_selection'):
        st.session_state.root_directory_selection = "/Users/mcfatem"
    if not state('google_sheet_selection'):
        st.session_state.google_sheet_selection = None
    if not state('google_sheet_url'):
        st.session_state.google_sheet_url = None
    if not state('google_worksheet_selection'):
        st.session_state.google_worksheet_selection = None
    if not state('worksheet_column_selection'):
        st.session_state.worksheet_column_selection = None
    if not state('worksheet_column_number'):
        st.session_state.worksheet_column_number = None
    if not state('stfs_path_selection'):
        st.session_state.stfs_path_selection = None
    if not state('use_previous_file_list'):
        st.session_state.use_previous_file_list = False
    if not state('check_worksheet_column_headings'):
        st.session_state.check_worksheet_column_headings = False
    if not state('regex_text'):
        st.session_state.regex_text = False
    if not state('output_to_csv'):
        st.session_state.output_to_csv = False
    if not state('generate_thumb'):
        st.session_state.generate_thumb = False
    if not state('generate_small'):
        st.session_state.generate_small = False
    if not state('processing_mode'):
        st.session_state.processing_mode = False
    if not state('azure_blob_storage'):
        st.session_state.azure_blob_storage = False
    if not state('transfer_transcripts'):
        st.session_state.transfer_transcripts = False
    if not state('copy_to_objs'):
        st.session_state.copy_to_objs = False
    if not state('dump_dataframe'):
        st.session_state.dump_dataframe = False
    if not state('df'):
        st.session_state.df = pd.DataFrame( )  # Empty Pandas dataframe for our Google Sheet

    # Display and fetch options from the sidebar
    with st.sidebar:

        # # Processing mode
        # processing_mode = st.radio(
        #     "Select desired processing mode",
        #     ["None", "Migration to Alma", "CollectionBuilder"],
        #     key="processing_mode_radio",
        #     disabled=False)
        # st.session_state.processing_mode = processing_mode
        # if state('processing_mode') == "None":
        #     st.session_state.processing_mode = False

        st.session_state.processing_mode = "CollectionBuilder"     # Always the case in this app!

        # Use previous file list?
        use_previous_file_list = st.checkbox(
            label=
            "Check here to use the previous list of filenames stored in 'file-list.tmp'",
            value=False,
            key='use_previous_file_list_checkbox')
        st.session_state.use_previous_file_list = use_previous_file_list

        # # Check worksheet column headings
        # if state('use_previous_file_list'):
        #     st.session_state.check_worksheet_column_headings = False
        #     check_worksheet_column_headings = st.checkbox(
        #         label="Check worksheet for proper column headings",
        #         value=False,
        #         disabled=True,   
        #         key='check_worksheet_column_headings_checkbox')
        # else:
        # st.session_state.check_worksheet_column_headings = st.checkbox(
        #     label="Check worksheet for proper column headings",
        #     value=state('check_worksheet_column_headings'),
        #     disabled=False,
        #     key='check_worksheet_column_headings_checkbox')

        # Output to CSV?
        output_to_csv = st.checkbox(
            label="Check here to output results to a CSV file",
            value=False,
            key='output_to_csv_checkbox')
        st.session_state.output_to_csv = output_to_csv

        # Limit search with regex?
        regex_text = st.text_input(label= "Specify a 'regex' pattern here to limit the scope of your search", value=None,key='regex_text_input')
        st.session_state.regex_text = regex_text

        # Copy files to Azure blob storage?
        azure_blob_storage = st.checkbox(
            "Check here to copy EXACT found files and derivatives to Azure Blob Storage",
            value=False,
            key='azure_blob_storage_checkbox')
        st.session_state.azure_blob_storage = azure_blob_storage

        # Search for Transcript files
        if state('azure_blob_storage'):
            transfer_transcripts = st.checkbox(
                "Check here to search for and transfer CSV, VTT, PDF, or XML transcript files",
                value=False,
                key='transfer_transcripts_checkbox',
                disabled=False)
            st.session_state.transfer_transcripts = transfer_transcripts

        else:
            transfer_transcripts = st.checkbox(
                "Check here to search for and transfer CSV, VTT, PDF, or XML transcript files",
                value=False,
                key='transfer_transcripts_checkbox',
                disabled=False)
            st.session_state.transfer_transcripts = False

        # Generate thumbnail and/or small image derivatives?
        if state('azure_blob_storage'):
            generate_thumb = st.checkbox(
                "Check here to automatically generate and save thumbnail (TN) images",
                value=False,
                key='generate_thumb_checkbox',
                disabled=False)
            st.session_state.generate_thumb = generate_thumb
        
        else: 
            generate_thumb = st.checkbox(
                "Check here to automatically generate and save thumbnail (TN) images",
                value=False,
                key='generate_thumb_checkbox',
                disabled=True)
            st.session_state.generate_thumb = False

        if state('azure_blob_storage'):
            generate_small = st.checkbox(
                "Check here to automatically generate and save small (JPG) images",
                value=False,
                key='generate_small_checkbox',
                disabled=False)
            st.session_state.generate_small = generate_small

        else: 
            generate_small = st.checkbox(
                "Check here to automatically generate and save small (JPG) images",
                value=False,
                key='generate_small_checkbox',
                disabled=True)
            st.session_state.generate_small = False

        # # Copy OBJ files (and clientThumbs) to //Volumes/exports/OBJs for Alma?
        # if state('azure_blob_storage'):
        #     copy_to_objs = st.checkbox(
        #         "Check here to copy found OBJ (and clientThumb) files to //Volumes/exports/OBJs for Alma",
        #         value=False,
        #         key='copy_to_objs_checkbox',
        #         disabled=True)
        #     st.session_state.copy_to_objs = copy_to_objs

        # else:
        #     copy_to_objs = st.checkbox(
        #         "Check here to copy found OBJ (and clientThumb) files to //Volumes/exports/OBJs for Alma",
        #         value=False,
        #         key='copy_to_objs_checkbox',
        #         disabled=False)
        #     st.session_state.copy_to_objs = copy_to_objs

        # Dump dataframe before save?
        dump_dataframe = st.checkbox(
            "Dump the Google Sheet dataframe before it is saved?",
            value=False,
            key='dump_dataframe_checkbox')
        st.session_state.dump_dataframe = dump_dataframe

    # Fetch the --worksheet argument
    if not state('use_previous_file_list'):
        get_worksheet_column_selection( )

    # Fetch the --tree-path argument
    get_tree()

    # Check parameters to see if we have enough input to run a search
    go1 = state('use_previous_file_list') and state('stfs_path_selection')
    go2 = state('google_sheet_url') and state(
        'google_worksheet_selection') and state(
            'worksheet_column_number') and state('stfs_path_selection')

    msg = ""

    # Run a search using previous list of filenames
    if go1:
        msg = f"using the previous list of filenames AND specified directory: {state('stfs_path_selection')}"
        txt = f"Fuzzy search is **ready**... **{msg}**"
        st.success(txt)
        state('logger').success(txt)

    # Fetch new filenames for a pristine search
    elif go2:
        msg = f"using the filenames from column \'{state('worksheet_column_selection')}\' of worksheet \'{state('google_worksheet_selection')}\' AND specified directory: {state('stfs_path_selection')}"
        txt = f"Fuzzy search is **ready**... **{msg}**"
        st.success(txt)
        state('logger').success(txt)

    # Not ready for prime time
    else:
        txt = f"Fuzzy search parameters are incomplete!"
        st.warning(txt)
        state('logger').warning(txt)

        st.write(f"Session state dump follows...")
        st.session_state

    # Ready... prompt for button press to run the search
    if go1 or go2:
        if st.button("Click HERE to run the search!", key='initiate_search_button'):
            with st.status(f"Go! {msg}") as status:
                csv_results = fuzzy_search_for_files(status)

            # Post-processing...
            if state('azure_blob_storage') or state('processing_mode'):
                post_processing(csv_results)
