import storage
import model
import os
import shutil

class BaoTrainingException(Exception):
    pass

def train_and_swap(fn, old, tmp, bao_db, verbose=False):
    if os.path.exists(fn):
        old_model = model.BaoRegression(have_cache_data=True)
        old_model.load(fn)
    else:
        old_model = None

    new_model = train_and_save_model(tmp, bao_db, verbose=verbose)

    if os.path.exists(fn):
        shutil.rmtree(old, ignore_errors=True)
        os.rename(fn, old)
    os.rename(tmp, fn)

def train_and_save_model(fn, bao_db, verbose=True, emphasize_experiments=0):
    all_experience = storage.experience(bao_db)

    for _ in range(emphasize_experiments):
        all_experience.extend(storage.experiment_experience(bao_db))
    
    x = [i[0] for i in all_experience]
    y = [i[1] for i in all_experience]        
    
    if not all_experience:
        raise BaoTrainingException("Cannot train a Bao model with no experience")
    
    if len(all_experience) < 20:
        print("Warning: trying to train a Bao model with fewer than 20 datapoints.")

    reg = model.BaoRegression(have_cache_data=True, verbose=verbose)
    reg.fit(x, y)
    reg.save(fn)
    return reg


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: train.py MODEL_FILE")
        exit(-1)
    train_and_save_model(sys.argv[1])

    print("Model saved, attempting load...")
    reg = model.BaoRegression(have_cache_data=True)
    reg.load(sys.argv[1])

