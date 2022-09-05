import pandas as pd
import os
from lxml import etree
import logging


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
        if is_501c3(root):
            ein = get_ein(root)
            orgs[ein] = get_vars(root, variables)
    df = get_df(orgs)
    df.to_csv(path)


def is_501c3(root):
    tag = root.xpath(".//n:*[contains(local-name(), 'Organization501c3')]", namespaces=NAMESPACES)
    if tag is not None:
        if len(tag) > 0:
            if tag[0].text == 'X':
                return True
    return False


def get_df(dictionary):
    df = pd.DataFrame.from_dict(dictionary, orient='index')
    df.reset_index()
    df.rename(columns={"index": "ein"})
    return df


def get_root(file):
    with open(file) as f:
        try:
            return etree.parse(f).getroot()
        except (UnicodeDecodeError, etree.XMLSyntaxError) as e:
            logging.warning(f'When parsing {file}, {e} occurred')


def get_ein(root):
    return get_var(root, Variable('EIN', 'EIN'))


def get_vars(root, variables):
    vars_dict = {}
    for var in variables:
        var_text = get_var(root, var)
        if var_text:
            vars_dict[var.name] = var_text
        else:
            logging.debug(f'{root.base.replace("/Users/erichegonzales/ECON_298/test_clean/", "")} is missing {var.name}')
    return vars_dict


def get_var(root, var):
    if var.is_multiline:
        text = ''.join(
            [line.text for line in root.find(f'.//n:{var.tag}', namespaces=NAMESPACES) if line is not None])
    elif type(var.tag) == str:
        text =  root.findtext(f'.//n:{var.tag}', namespaces=NAMESPACES)
    else:
        text = get_var_from_list_of_tags(root, var)
    return text


def get_var_from_list_of_tags(root, var):
    text = None
    idx = 0
    while text is None and idx < len(var.tag):
        text = root.findtext(f'.//n:{var.tag[idx]}', namespaces=NAMESPACES)
        idx += 1
    return text

def load_animation(idx):
    if idx != 0 and idx % 1000 == 0:
        logging.info(f'{idx} files processed')


logging.basicConfig(level=logging.INFO)

VARIABLES = [Variable('Name', 'BusinessName', multiline=True),
             Variable('State', 'StateAbbreviationCd'),
             Variable('Mission Statement', 'ActivityOrMissionDesc'),
             Variable('ZIP', 'ZIPCd'),
             Variable('Primary Exempt Purpose', 'PrimaryExemptPurposeTxt'),
             Variable('Description', 'ProgramSrvcAccomplishmentGrp/n:DescriptionProgramSrvcAccomTxt'),
             Variable('Total Assets EOY', ['Form990TotalAssetsGrp/n:EOYAmt', 'TotalAssetsEOYAmt']),
             Variable('Total Liabilities EOY', 'SumOfTotalLiabilitiesGrp/n:EOYAmt'),
             Variable('Federated Campaigns', 'PoliticalCampaignActyInd'),
             Variable('Program Service Revenue', 'ProgramServiceRevenueAmt'),
             Variable('Other Revenue', 'OtherRevenueTotalAmt'),
             Variable('Total Revenue', 'TotalRevenueAmt'),
             Variable('Total Program Services Expenses', 'TotalProgramServiceExpensesAmt'),
             Variable('Total Fundraising Expenses', 'TotalProgramServiceExpensesAmt'),
             Variable('Total Expenses', 'TotalExpensesAmt'),
             Variable('Other Expenses', 'OtherExpensesTotalAmt'),
             Variable('Tax Period Begin Date', 'TaxPeriodBeginDt')]
DIRECTORY = '/Users/erichegonzales/ECON_298/test_clean'
NAMESPACES = {'n': 'http://www.irs.gov/efile'}
OUTPUT_PATH = '/Users/erichegonzales/ECON_298/2020_Orgs.csv'
if __name__ == '__main__':
    get_and_save_orgs_df(DIRECTORY, VARIABLES, OUTPUT_PATH)




