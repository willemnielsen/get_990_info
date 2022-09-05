import pandas as pd
import os
from lxml import etree


class OrgsPipeline:
    def __init__(self, directory, variables, throw_out_if_error=True):
        self.directory = directory
        self.details = variables
        self.orgs = self._get_501c3_orgs(throw_out_if_error)
        self.df = self._get_orgs_df()

    def _get_501c3_orgs(self, throw_out_if_error):
        orgs = []
        for idx, file in enumerate(os.listdir(self.directory)):
            OrgsPipeline.load_animation(idx)
            org = Org(file, self.details)
            orgs.append(org)
        return orgs

    def _get_orgs_df(self):
        orgs_dict = self._get_orgs_with_variables_dict()
        return self.get_dataframe_from_dict(orgs_dict)

    def _get_orgs_with_variables_dict(self):
        orgs_dict = {}
        for org in self.orgs:
            orgs_dict[org.ein] = org.details_to_dict()
        return orgs_dict

    def to_csv(self, path):
        self.df.to_csv(path)

    @staticmethod
    def get_dataframe_from_dict(dictionary):
        return pd.DataFrame.from_dict(dictionary, orient='index').reset_index().rename(columns={"index": "ein"})

    @staticmethod
    def load_animation(idx):
        if idx != 0 and idx % 1000 == 0:
            return f'{idx} files processed'


class Org:
    def __init__(self, file, details, stop_if_error=True):
        self.file = file
        self._root = self.get_root()
        self.ein = self._root.findtext(f'.//n:EIN', namespaces=NAMESPACES)
        self.is_501c3 = self._is_501c3()
        self.details = details
        self._set_detail_values()

    def get_root(self):
        with open(self.file) as f:
            return etree.parse(f).getroot()

    def _set_detail_values(self):
        for detail in self.details:
            detail.value = self._get_value_for(detail)

    def _get_value_for(self, detail):
        if detail.is_multiline:
            lines = self._root.find(f'.//n:{detail.tag}', namespaces=NAMESPACES)
            text = ''.join([line.text for line in lines if line is not None])
        elif type(detail.tag) == str:
            text = self._root.findtext(f'.//n:{detail.tag}', namespaces=NAMESPACES)
        else:
            text = self._root.findtext(f'.//n:{detail.tag}', namespaces=NAMESPACES)
        return text

    def details_to_dict(self):
        return {detail.name: detail.value for detail in self.details}

    def _is_501c3(self):
        tag = self._root.xpath(".//n:*[contains(local-name(), 'Organization501c3')]", namespaces=NAMESPACES)
        if tag is not None:
            if len(tag) > 0:
                if tag[0].text == 'X':
                    return True
        return False


class Detail:
    def __init__(self, name, tag, multiline=False):
        self.name = name
        self.tag = tag
        self.is_multiline = multiline
        self.value = None




def save_orgs(path):
    orgs = OrgsPipeline(path, details_to_extract)
    orgs.df.to_csv('../2020_501c3_missionstatements.csv')



class Get:
    @staticmethod
    def get_detail_from_990(root, tag_or_get):
        if callable(tag_or_get):
            return tag_or_get(root)
        else:
            return root.find(f'.//n:{tag_or_get}', namespaces=NAMESPACES).text

    @staticmethod
    def total_assets(root):
        root.find(f'.//n:Form990TotalAssetsGrp/', namespaces=NAMESPACES)


NAMESPACES = {'n': 'http://www.irs.gov/efile'}

details_to_extract = [Detail('Name', 'BusinessName', multiline=True),
                      Detail('State', 'StateAbbreviationCd'),
                      Detail('Mission Statement', 'ActivityOrMissionDesc'),
                      Detail('ZIP', 'ZIPCd'),
                      Detail('Total Assets EOY', 'Form990TotalAssetsGrp/EOYAmt'),
                      # Detail('Total Liabilities EOY', 'SumOfTotalLiabilitiesGrp/EOYAmt'),
                      # Detail('Federated Campaigns', 'PoliticalCampaignActyInd'),
                      # Detail('Program Service Revenue', 'ProgramServiceRevenueAmt'),
                      # Detail('Other Revenue', 'OtherRevenueTotalAmt'),
                      # Detail('Total Revenue', 'TotalRevenueAmt'),
                      # Detail('Total Program Services Expenses', 'TotalProgramServiceExpensesAmt'),
                      # Detail('Total Fundraising Expenses', 'TotalProgramServiceExpensesAmt'),
                      # Detail('Total Expenses', 'TotalExpensesAmt'),
                      # Detail('Other Expenses', 'OtherExpensesTotalAmt'),
                      # Detail('Tax Period Begin Date', 'TaxPeriodBeginDt' ),
                      Detail('Tax Year', 'TaxYr')
                      # Detail('Membership Dues', 'MembershipDuesAmt')
                      ]

# charity name
# state
# ein
# missions statement
# taxyear (year the taxes were being paid for)
# taxmonth (month the taxes were being paid in)
# zip code
# total assets
# total contributions
# ntee code
# year of formation


if __name__ == '__main__':
    orgs_pipeline = OrgsPipeline('.', details_to_extract)
    save_orgs('..')
