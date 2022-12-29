import os
import pathlib as pl

def test_file_exists():
    baba_dir = '/wsefs/pipeline/babadata/'
    file_name = 'etfca_2022102.tif'
    full_file_name = os.path.join(baba_dir, file_name)
    path = pl.Path(full_file_name)

    assert (path.is_file())

def null_test_file_exists_fail():
    baba_dir = '/wsefe/pipeline/babadata/'
    file_name = 'etfca_2022102.tif'
    full_file_name = os.path.join(baba_dir, file_name)
    path = pl.Path(full_file_name)

    assert (path.is_file())

def test_one_two():
    assert 1==1


#print(type(path))
#print(dir(path))
