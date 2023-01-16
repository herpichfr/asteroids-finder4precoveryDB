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
    """Read and format MPC database to a table format"""
    df = pd.read_json(mpcddbpath)
    # select only more recent observations to avoid crashing pyorb
    mask = df['Last_obs'] > '2015-01-01'
    df2 = df[["Number", "Name", "a", "e", "i", "Node", "Peri", "M", "Epoch", "H", "G", "Last_obs"]][mask]

    return df2

def search_orbit_inDB(lock, mpcdf, indexes, savedirpath):
    """Search MPC orbit within precovery DB"""
    for index in indexes:
        if index == -99:
            print('This is a filler index. Skipping')
        else:
            outfilename = os.path.join(savedirpath, 'mpc_ast' + repr(1000000 + index) + '.csv')
            checkfilename = os.path.join(savedirpath, 'all_tested_asts.csv')
            if os.path.isfile(outfilename):
                print(outfilename, 'already exists. Skipping...')
            else:
                print('calculating orbits for index', index)
                df = mpcdf.iloc[index]
                orbit = Orbit.keplerian(
                    0,
                    df['a'], df['e'], df['i'], df['Node'], df['Peri'], df['M'],
                    df['Epoch'] - 2400000.5,
                    EpochTimescale.TT,
                    20,
                    0.15)

                results = precover(orbit, DB_DIR, tolerance=1/3600)
                print(results)
                # need to save dataframe
                if len(results) > 0:
                    if not os.path.isdir(savedirpath):
                        os.mkdir(savedirpath)
                    print('saving results for index', index, 'to', outfilename)
                    results.to_csv(outfilename, index=False)

                lock.acquire(timeout=5)
                with open(checkfilename, 'a') as checkfile:
                    checkfile.write('%i,%i\n' % (index, len(results)))
                    checkfile.close()
                lock.release()

    return

    # Press the green button in the gutter to run the script.
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
    # Precovery DB path
    DB_DIR = '/epyc/ssd/users/herpich2/splus_idr4_nomatch_new/'
    # create file to save checked asteroids
    if not os.path.isfile(os.path.join(savedirpath, 'all_tested_asts.csv')):
        with open(os.path.join(savedirpath, 'all_tested_asts.csv'), 'w') as creating_new_csv_file:
            creating_new_csv_file.write('Index,Size\n')
            creating_new_csv_file.close()

    num_procs = 40
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

    lock = multiprocessing.Lock()
    for ind in indexes:
        process = multiprocessing.Process(target=search_orbit_inDB, args=(lock, mpcdf, ind, savedirpath))
        jobs.append(process)

    # start jobs
    print('starting', num_procs, 'jobs!')
    for j in jobs:
        j.start()

    # check if any of the jobs initialized previously still alive
    # save resulting table after all are finished
    proc_alive = True
    while proc_alive:
        if any(proces.is_alive() for proces in jobs):
            proc_alive = True
            time.sleep(1)
        else:
            print('All jobs finished')
            proc_alive = False

    print('Done!')