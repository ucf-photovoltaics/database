# -*- coding: utf-8 -*-
"""
Utility functions module.

Author: Brent
"""

import numpy as np
import logging
import os
import shutil
import tkinter as tk
from tkinter import filedialog

# Configuration
LOG_PATH = os.getenv("LOG_PATH", "C:/Users/Doing/University of Central Florida/UCF_Photovoltaics_GRP - module_databases/FSEC_Database_log.log")



def deserialize_array(blob, dtype=np.float64):
    """
    Deserialize arrays encoded during storage.

    Parameters:
    blob (bytes): Serialized array.
    dtype (np.dtype): Data type of the array.

    Returns:
    np.ndarray: Deserialized array.
    """
    try:
        return np.frombuffer(blob, dtype=dtype)
    except Exception as e:
        logger.error("Error deserializing array: %s", str(e))
        return None

def get_files(title='Select files'):
    """
    Prompt user to select file or files.

    Parameters:
    title (str): Title on the dialog box.

    Returns:
    list: Full file paths of selected files.
    """
    root = tk.Tk()
    try:
        files = list(filedialog.askopenfilenames(title=title))
    except Exception as e:
        logger.error("Error selecting files: %s", str(e))
        files = []
    root.destroy()
    return files

def get_filename_metadata(file, datatype='iv'):
    """
    Extract metadata from the filename string based on FSEC PVMCF filename standards.

    Parameters:
    file (str): File path string.
    datatype (str): Type of measurement data.

    Returns:
    dict: Dictionary of metadata obtained from the filename string.
    """
    metadata_dict = {}
    try:
        bn_split = os.path.basename(file).split('_')
        ext = file.split('.')[-1]

        common_metadata = {
            'date': bn_split[0],
            'time': bn_split[1],
            'make': bn_split[2],
            'model': bn_split[3],
            'serial_number': bn_split[4],
            'comment': bn_split[5].split('.')[0]
        }

        if datatype == 'iv':
            metadata_dict.update(common_metadata)
            metadata_dict.update({
                'measurement_number': bn_split[6].replace(f".{ext}", '')
            })
        elif datatype == 'el':
            metadata_dict.update(common_metadata)
            metadata_dict.update({
                'exposure_time': bn_split[6].replace('s', ''),
                'current': bn_split[7].replace('A', ''),
                'voltage': bn_split[8].replace(f"V.{ext}", '')
            })
        elif datatype == 'ir':
            metadata_dict.update(common_metadata)
            metadata_dict.update({
                'exposure_time': bn_split[6].replace('s', ''),
                'current': bn_split[7].replace(f"A.{ext}", '')
            })
        elif datatype == 'dark_iv':
            metadata_dict.update(common_metadata)
        elif datatype == 'uvf':
            metadata_dict.update(common_metadata)
        elif datatype == 'v10':
            metadata_dict = {
                'serial-number': bn_split[4],
                'date': bn_split[0],
                'time': bn_split[1],
                'delay-time-(s)': bn_split[6].split('s')[0],
                'setpoint-total-time-(s)': bn_split[5].replace('s', '')
            }
        elif datatype == 'scanner':
            metadata_dict.update(common_metadata)
            metadata_dict.update({
                'module_id': bn_split[2],
                'exposure_time': bn_split[6],
                'current': bn_split[7],
                'voltage': bn_split[8],
                'image_type': bn_split[10].split('.')[0] if ext == 'jpg' else None,
                'cell_number': bn_split[11] if ext == 'jpg' and bn_split[10].split('.')[0] == 'cell' else None
            })
    except Exception as e:
        logger.error("Error extracting metadata: %s", str(e))
    return metadata_dict

def search_folders(date_threshold=20000000, parent_folder_path=''):
    """
    Uses a date threshold to select all folders in given parent path that beyond the given date.

    Parameters:
    date_threshold (int): Date threshold.
    parent_folder_path (str): Parent folder path.

    Returns:
    list: List of folders beyond the given date.
    """
    if not os.path.isdir(parent_folder_path):
        parent_folder_path = filedialog.askdirectory(title='Select source of data files to search through.')

    folders = []
    for dirpath, dirnames, filenames in os.walk(parent_folder_path):
        for dirname in dirnames:
            dirname = dirname.replace('-', '')
            try:
                if int(dirname) >= int(date_threshold):
                    new_folder = os.path.join(dirpath, dirname)
                    folders.append(new_folder)
                    logger.info('%s added for processing.', new_folder)
            except ValueError:
                logger.warning('%s skipped.', dirname)
    return folders

def get_directory_names(source):
    """
    Uses os.walk to return a list of directories.

    Parameters:
    source (str): Source directory path.

    Returns:
    list: List of directory names.
    """
    directory_names = []
    try:
        for dirpath, dirnames, filenames in os.walk(source):
            for name in dirnames:
                directory_names.append(name)
                logger.info(name)
    except Exception as e:
        logger.error("Error retrieving directory names: %s", str(e))
    return directory_names