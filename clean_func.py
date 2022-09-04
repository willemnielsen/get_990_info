import pandas as pd
import os
from lxml import etree


class Variable:
    def __init__(self, name, tag, multiline=False):
        self.name = name
        self.tag = tag
        self.is_multiline = multiline


def get_and_save_orgs_df(directory, variables, path):
    orgs = {}
    for idx, file in enumerate(os.listdir(directory)):
        load_animation(idx)
        root = get_root(file)
        ein = get_ein(root)
        try:
            orgs[ein] = get_vars(root, variables)
        except AttributeError:
            pass
    df = get_df(orgs)
    df.to_csv(path)


def get_df(dictionary):
    df = pd.DataFrame.from_dict(dictionary, orient='index')
    df.reset_index()
    df.rename(columns={"index": "ein"})
    return df


def get_root(file):
    with open(file) as f:
        return etree.parse(f).getroot()


def get_ein(root):
    return get_var(root, Variable('EIN', 'EIN'))


def get_vars(root, variables):
    return {var.name: get_var(root, var) for var in variables}


def get_var(root, var):
    try:
        if var.is_multiline:
            return ''.join(
                [line.text for line in root.find(f'.//n:{var.tag}', namespaces=NAMESPACES)])
        else:
            return root.find(f'.//n:{var.tag}', namespaces=NAMESPACES).text
    except AttributeError:
        pass


def load_animation(idx):
    if idx != 0 and idx % 1000 == 0:
        return f'{idx} files processed'


VARIABLES = [Variable('Name', 'BusinessName', multiline=True),
             Variable('State', 'StateAbbreviationCd'),
             Variable('Mission Statement', 'ActivityOrMissionDesc'),
             Variable('ZIP', 'ZIPCd'),
             Variable('Total Assets EOY', 'Form990TotalAssetsGrp/EOYAmt'),
             Variable('Tax Year', 'TaxYr')]
DIRECTORY = os.path.expanduser('test_clean')
NAMESPACES = {'n': 'http://www.irs.gov/efile'}
OUTPUT_PATH = 'Users/erichegonzales/test_clean'
if __name__ == '__main__':
    get_and_save_orgs_df(DIRECTORY, VARIABLES, OUTPUT_PATH)




