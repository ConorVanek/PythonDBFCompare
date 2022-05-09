import os
# import pandas as pd
from glob import glob
from shutil import copy

from dbfread import DBF
from pandas import DataFrame, concat


def create_folders():
    # Check whether the specified path exists or not
    path = '../Data/compare_result'
    isExist = os.path.exists(path)

    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Created results table.")

    path = '../Data/snapshot'
    isExist = os.path.exists(path)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path)
        print("Created snapshot table.")

def get_dbf_files(directory, allExtensions):
    listfile = open('files.txt', 'r')
    lines = listfile.readlines()
    files = []
    for file in lines:
        files.extend([f for f in glob(directory + '/' + file.strip() + '.dbf')])
        if allExtensions:
            files.extend([f for f in glob(directory + '/' + file.strip() + '.cdx')])
            files.extend([f for f in glob(directory + '/' + file.strip() + '.fpt')])
            files.extend([f for f in glob(directory + '/' + file.strip() + '.cdx')])
            files.extend([f for f in glob(directory + '/' + file.strip() + '.tmp')])
        
    return files

def copy_to_snapshot_folder(files):
    snapshot_directory = '../Data/snapshot'
    for file in files:
        f2 = file.split('\\')[-1]
        print(f2)
        copy(file, snapshot_directory)

def restore_from_snapshot_folder():
    files = get_dbf_files('../Data/snapshot', True)
    dst_directory = '../Data'
    for file in files:
        try:
            copy(file, dst_directory)
        except Exception as e:
            print(e)

def clear_matches():
    dir = '../Data/compare_result'
    for f in os.listdir(dir):
        try:
            os.remove(os.path.join(dir, f))
        except Exception as e:
            print(e)

def clear_snapshot():
    dir = '../Data/snapshot'
    for f in os.listdir(dir):
        try:
            os.remove(os.path.join(dir, f))
        except Exception as e:
            print(e)

def compare_dbfs(file1, file2, compare_type):
    result_dir = '../Data/compare_result/'
    filename = file1.split('/')[-1].split('.')[0] + '.csv'
    print("Loading " + filename + ". . .\n")
    dbf1 = DBF(file1, ignore_missing_memofile=True, char_decode_errors='replace')
    dbf2 = DBF(file2, ignore_missing_memofile=True, char_decode_errors='replace')
    print("Converting " + filename + " to dataframe. . .\n")
    frame1 = DataFrame(iter(dbf1))
    frame2 = DataFrame(iter(dbf2))
    print("comparing " + filename + ". . .\n")
    if compare_type == "matches":
        matches = frame1.merge(frame2, how = 'inner', indicator=False)
        if not matches.empty:
            matches.to_csv(result_dir + filename)
    elif compare_type == "differences":
        differences = concat([frame1,frame2]).drop_duplicates(keep=False)
        if differences.empty is False:
            differences.to_csv(result_dir + filename)
    else:
        return ""

def begin():
    #differences = compare_dbfs('./compare_test/pols.dbf', './compare_test/pols2.dbf', 'differences')
    #differences
    create_folders()
    command = ''
    while command != 'q':
        print("\n\n------DBF File Difference Checker------\n\n")
        print("Commands: \n")
        print("s: take snapshot of files")
        print("m: compare files to snapshot and get matches")
        print("d: compare files to snapshot and get differences")
        print("r: restore from snapshots")
        print("cm: clear results folder")
        print("cs: clear snapshot")
        print("q: quit")

        command = input('\n> ')
        if command == 's':
            try:
                files = get_dbf_files('../Data', True)
                copy_to_snapshot_folder(files)
            except Exception as e:
                print(e)
        elif command == 'm':
            clear_matches()
            snapshot_files = get_dbf_files('../Data/snapshot', False)
            ksv_files = []
            for f in snapshot_files:
                ksv_files.append('../Data/' + f.split('\\')[-1])
            for i in range(len(snapshot_files)):
                try:
                    compare_dbfs(ksv_files[i], snapshot_files[i], 'matches')
                except Exception as e:
                    print(e)

        elif command == 'd':
            clear_matches()
            snapshot_files = get_dbf_files('../Data/snapshot', False)
            ksv_files = []
            for f in snapshot_files:
                ksv_files.append('../Data/' + f.split('\\')[-1])
            for i in range(len(snapshot_files)):
                try:
                    compare_dbfs(ksv_files[i], snapshot_files[i], 'differences')
                except Exception as e:
                    print(e)
        elif command == 'r':
            restore_from_snapshot_folder()
        elif command == 'cs':
            clear_snapshot()
        elif command == 'cm':
            clear_matches()
        elif command == 'q':
            return
        else:
            print('Invalid Input. Please try again.')
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    begin()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
