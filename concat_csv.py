import argparse
import pandas as pd
import os
import yaml 

def append_to_df(combined_file : str, source_file : str, replace_oldest=False):
    """[summary]

    Args:
        combined_file (str): path to .csv file that is appended to
        source_file (str): path to .csv file which contains data to append
        replace_oldest (bool, optional): Whether to replace oldest columns. Defaults to False.
    """
    combined_df = pd.read_csv(combined_file)
    source_df = pd.read_csv(source_file)
    source_labels = list(source_df.columns)
    combined_labels = list(combined_df.columns)
    starting_index = source_labels.index("address 1")
    combined_df = combined_df.rename(columns={combined_labels[i] : combined_labels[i] + "OLD" for i in range(starting_index, len(combined_labels))})
    to_copy = source_df.iloc[:, starting_index : ]
    for label in source_labels[starting_index : ]:
        combined_df = combined_df.join(source_df[label])
    
    if replace_oldest:
        amt_to_remove = len(source_labels) - starting_index
        combined_df = combined_df.drop(columns=combined_df.columns[starting_index : starting_index + amt_to_remove])

    col_types = ["address", "timestamp", "inquiry order"]
    combined_df = combined_df.rename(columns={combined_df.columns[i] : col_types[(i-starting_index)%len(col_types)] + " " + str((i-starting_index)//len(col_types) + 1) for i in range(starting_index,len(combined_df.columns))})
    combined_df.to_csv(combined_file,index=False)

def get_source_files(rounds_folder):
    files = os.listdir(rounds_folder)
    updated_files = [os.path.join(rounds_folder,f) for f in files if f.split("_")[-1] == "updated.csv"]
    return updated_files

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy between csv files')
    config = yaml.load(open("config.yaml", "r"))
    parser.add_argument('round_num', type=int, help="Round number to combine")
    parser.add_argument('replace', type=str2bool, nargs='?', const=True, default=False, help="Whether to delete oldest columns")
    args = parser.parse_args()
    round_folder = os.path.join(config["rounds_path"],f"round_{args.round_num}")
    source_files = get_source_files(round_folder)
    dest_file = os.path.join(round_folder, config["concat_file"])
    for s in source_files:
        append_to_df(dest_file, s, args.replace)
