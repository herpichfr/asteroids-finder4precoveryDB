# 2022-12-08 @herpichfr
# Herpich, Fabio R - fabiorafaelh@gmail.com
#
import os
import numpy as np
import pandas as pd
from precovery.orbit import Orbit, EpochTimescale
from precovery.main import precover
import multiprocessing
import time

def read_mpc_database(mpcddbpath):
    """
    Read and format MPC database to a table format

    Parameters:
        ----------
        mpcddbpath: Pandas dataframe from MPC

    Returns:
        ----------
        df: dataframe filtered for observations older than 2015-01-01
    """
    df = pd.read_json(mpcddbpath)
    # select only more recent observations to avoid crashing pyorb
    mask = df['Last_obs'] > '2015-01-01'
    df2 = df[["Number", "Name", "a", "e", "i", "Node", "Peri", "M", "Epoch", "H", "G", "Last_obs"]][mask]

    return df2

def search_orbit_inDB(lock, mpcdf, indexes, savedirpath):
    """Search MPC orbit within a Precovery DB

    Parameters:
        ----------
        lock: processing Lock() object to synchronize processes

        mpcdf: Pandas dataframe containing data from MPC catalogue

        indexes: list of indexes from mpcdf

        savedirpath: path to save the results
    """
    for index in indexes:
        if index == -99:
            # skip indexes -99 added artificially for the sake of having same size lists for multiprocessing
            print('This is a filler index. Skipping')
        else:
            # create paths for files to be saved
            outfilename = os.path.join(savedirpath, 'mpc_ast' + repr(1000000 + index) + '.csv')
            checkfilename = os.path.join(savedirpath, 'all_tested_asts.csv')
            if os.path.isfile(outfilename):
                # skip already existing files
                print(outfilename, 'already exists. Skipping...')
            else:
                # get data relative to the index
                print('calculating orbits for index', index)
                df = mpcdf.iloc[index]
                # get MPC orbit
                orbit = Orbit.keplerian(
                    0,
                    df['a'], df['e'], df['i'], df['Node'], df['Peri'], df['M'],
                    df['Epoch'] - 2400000.5,
                    EpochTimescale.TT,
                    20,
                    0.15)

                # search within the Precovery DB for that orbit
                results = precover(orbit, DB_DIR, tolerance=1/3600)
                print(results)

                # save results when dataframe is not empy
                if len(results) > 0:
                    if not os.path.isdir(savedirpath):
                        os.mkdir(savedirpath)
                    print('saving results for index', index, 'to', outfilename)
                    results.to_csv(outfilename, index=False)

                # completeness check file. Add to the last line of file all tests performed to allow further checks
                # of number of MPC entries tested
                lock.acquire(timeout=5)
                with open(checkfilename, 'a') as checkfile:
                    checkfile.write('%i,%i\n' % (index, len(results)))
                    checkfile.close()
                lock.release()

    return


if __name__ == '__main__':
    # get MPC database
    mpcdbpath = '/epyc/ssd/users/herpich2/mpcorb_extended.json.gz'
    mpcdf = read_mpc_database(mpcdbpath)

    # define paths
    # workdir
    workdir = '/astro/store/epyc3/projects3/splus_dataset/'
    # dir to save results of MPC search
    savedirpath = os.path.join(workdir, 'found_asteroids_20230116/')
    if not os.path.isdir(savedirpath):
        os.mkdir(savedirpath)
    # Precovery DB path. If not global, need to be explicitly defined before Orbit.keplerian is called
    DB_DIR = '/epyc/ssd/users/herpich2/splus_idr4_nomatch_new/'
    # create file to save checked asteroids
    if not os.path.isfile(os.path.join(savedirpath, 'all_tested_asts.csv')):
        with open(os.path.join(savedirpath, 'all_tested_asts.csv'), 'w') as creating_new_csv_file:
            creating_new_csv_file.write('Index,Size\n')
            creating_new_csv_file.close()

    # number of process to be considered
    num_procs = 40
    # create num_procs same size lists of indexes. Complete with -99 when needed to find numindexes % numprocs != 0
    l = list(mpcdf.index)
    numrows = len(l)
    if numrows % num_procs > 0:
        print('increasing number of indexes')
        increase_to = int(numrows / num_procs) + 1
        i = 0
        while i < (increase_to * num_procs - numrows):
            l.append(int(-99))
            i += 1
        else:
            print('number of lines already satisfy the conditions for num_proc')
    indexes = np.array(l).reshape((num_procs, int(np.array(l).size / num_procs)))

    print('calculating for a total of', indexes.size, 'rows')
    # create jobs
    jobs = []
    print('creating', num_procs, 'jobs...')

    lock = multiprocessing.Lock()  # needed to avoid different processes overstepping while writing file
    for ind in indexes:
        process = multiprocessing.Process(target=search_orbit_inDB, args=(lock, mpcdf, ind, savedirpath))
        jobs.append(process)

    # start jobs
    print('starting', num_procs, 'jobs!')
    for j in jobs:
        j.start()

    # check if any of the jobs initialized previously still alive
    proc_alive = True
    while proc_alive:
        if any(proces.is_alive() for proces in jobs):
            proc_alive = True
            time.sleep(1)
        else:
            print('All jobs finished')
            proc_alive = False

    print('Done!')