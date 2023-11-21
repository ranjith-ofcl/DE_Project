import os


name = 'stage/Album.csv'

file_name = name.split("/")
print(os.path.basename(name))