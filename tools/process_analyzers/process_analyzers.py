import os
from tools.process_analyzers.process_analyzer_lib import process_dir, register_signal_handler, is_processing_aborted

from multiprocessing import Pool, cpu_count
# from multiprocessing.pool import AsyncResult
# pool = Pool(processes=max(1, cpu_count() - 2))
pool = Pool(processes=3)

# base_dir = "/media/hwgsmb_home/KOM/DTN_SatSim/Simulations_IV_test"
base_dir = "/home/hendrik/KOM/DTN_SatSim/PONS/examples/output"
# base_dir = "/media/hwgsmb_home/KOM/DTN_SatSim/Simulations_II"

def main():
    register_signal_handler()
    for root, dirs, files in os.walk(base_dir):
        if is_processing_aborted():
            break
        for dir in dirs:
            # print(f"{dir}")
            if is_processing_aborted():
                break
            if os.path.exists(os.path.join(root, dir, "result.pkl")):
                # process_dir(root, dir) #Synchronous
                pool.apply_async(process_dir, [root, dir]) #Asynchronous
            else:
                # print('-- Simulation unfinished -> skipping')
                print(f'{dir}: Simulation unfinished -> skipping')

    pool.close()
    pool.join()

if __name__ == "__main__":
    main()