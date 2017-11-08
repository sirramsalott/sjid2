from os.path import join, isfile
from os import listdir
from string import punctuation
from sys import maxsize
from numpy import array
import xlrd, openpyxl, json, pickle
from company_name_similarity import CompanyNameSimilarity
from collections import defaultdict


# SET UP GLOBAL VARIABLES
document_home = "/home/joe/sajid/"
acquisitions_path = join(document_home, "M&As Data/Zephyr_Export_3.xls")
loans_path = join(document_home, "Syndicated Loan Data/")
loans_sheets_paths = []
cm = CompanyNameSimilarity()

for subdir in ["GlobalminusUSUK", "UK", "US"]:
    p = join(loans_path, subdir)
    loans_sheets_paths += [join(p, f) for f in listdir(p)
                           if isfile(join(p, f)) and not "~" in f and not "rev" in f]
num_loans = len(loans_sheets_paths)
    
stopwords = {"bank", "sa", "ltd", "inc", "plc", "of", "the", "ag",
             "oao", "de", "do", "di", "and",
             "banca", 's', 'co', 'banco', 'corp',
             'banque', 'corporation', 'international', 'credit',
             'bancorp', 'securities', 'commercial', 'na', 'in',
             'investment', 'savings', 'volksbank', 'branches', 'llc',
             'nv', 'assets', 'a', 'state', 'company',
             'raiffeisenbank', 'new', 'bk', 'shinkin', 'banka',
             'business', 'community', 'holdings', 'bancshares',
             'sparkasse', 'partners', 'united', 'insurance',
             'american', 'city', 'life', 'fund', 'credito', 'bhd',
             'kommercheskii', 'intl', 'services', 'hk', 'l',
             'leasing', 'holding', 'rural', 'management',
             'development', 'australia', 'cassa', 'merchant',
             'deutsche', 'markets', 'funding', 'lp', 'pt', 'd',
             'risparmio', 'operations', 'la', 'popolare', 'private',
             'zao', 'del', 'abn', 'ad', 'citizens', 'caisse', 'amro',
             'ahorros', 'e', 'hsbc', 'standard', 'rabobank', 'royal',
             'asset', 'industrial', 'societe', 'landesbank', 'ny',
             'fin', 'security', 'limited', 'mutual', 'du', 'suisse',
             'i', 'citibank', 'loan', 'dd', 'aktsionernyi', 'ab',
             'et', 'certain', 'texas', 'north', 'clo', 'und', 'natl',
             'ing', 'bnp', 'sumitomo', 'society', 'n', 'korea',
             'joint', 'south', 'mitsubishi', '1', 'county',
             'cooperativo', 'east', 'corporate', 'agricole', 'japan',
             'building', 'barclays', 'int', 'cdo', 'pat', 'generale',
             'france', 'europe', 'sg', 'retail', 'paribas', 'mitsui',
             'indonesia', 'banc', 'ua', 'islamic', 'bankers',
             'investments', 'dresdner', 'chartered', 'al', 'usa',
             'one', 'old', 'based', 'venture', 'ooo', 'general',
             'gmbh', 'southern', 'osuuspankki', 'western', 'overseas',
             'india', 'peoples', 'fsb', 'epargne', 'caixa', 'st',
             'invest', 'for', 'taiwan', 'malaysia', 'bv', 'tr', 'tbk',
             'cooperative', 'bankshares', 'sparebank', 'lyonnais',
             'grupo', 'cayman', 'lloyds', 'kreissparkasse', 'ins',
             'iii', 'global', 'ohio', 'schweiz', 'saudi', 'nord',
             'indosuez', 'export', 'ca', 'populaire', 'hypo',
             'georgia', 'crédit', 'austria', 'ubs', 'raiffeisen',
             'operative', 'bayerische', 'lynch'}
stopwords = {x for x in stopwords if len(x) < 5}


with open("wc.txt") as f:
    places = f.read().split("\n")
    

class Bank(object):

    similarity_threshold = 0.9
    
    def __init__(self, name):
        self.name = name
        (self.n_name, self.location) = self.normalise(name)
        self.name_set = set(self.n_name.split(" "))
        

    def __repr__(self):
        return self.name

    
    def normalise(self, name):
        tokens = ""
        locations = set()
        for x in name.lower().translate(str.maketrans(punctuation, " "*len(punctuation))).split(" "):
            #if x not in stopwords:
            if x in places:
                locations.add(x)
            else:
                tokens += x + " "
        return (tokens[:-1], locations)


    def jaccard(self, s1, s2):
        u = len(s1.union(s2))
        if u == 0:
            return 0
        return len(s1.intersection(s2)) / u

    
    def similarity(self, bank):
        if self.location != bank.location:
            return 0
        score = cm.match_score(self.n_name, bank.n_name, self.name_set, bank.name_set)
        return score
        

    def matches(self, bank, comparisons):
        return lookup_match(self.name, bank.name, comparisons) > self.similarity_threshold
    
    
class Loan(object):
    def __init__(self, num, date, leads, parts, borrower):
        self.num = num
        self.date = date
        self.leads = [Bank(l) for l in leads]
        self.parts = [Bank(p) for p in parts]
        self.all_managers = self.leads + self.parts
        self.borrower = borrower


    def __repr__(self):
        return str(self.__dict__)


    def find_matches(self, acquisitions, comparisons):
        matches = ([],[])
        for (k, ac) in enumerate(acquisitions):
            for (i, lead) in enumerate(self.leads):
                for (j, part) in enumerate(self.parts):    
                    if lead.matches(ac.acquiror, comparisons) and part.matches(ac.target, comparisons):
                        matches[0].append((i, j, ac.num))
                    if part.matches(ac.acquiror, comparisons) and lead.matches(ac.target, comparisons):
                        matches[0].append((j, i, ac.num))
        return matches                
    

class Acquisition(object):
    def __init__(self, num, acquiror, target, date, status):
        self.num = num
        self.acquiror = Bank(acquiror)
        self.target = Bank(target)
        self.date = date
        self.status = status


    def __repr__(self):
        return str(self.__dict__)


def is_lead(manager):
    return manager[1] in {"CO-MANAGER", "LEAD MANAGER", "CO-LEAD MANAGER"}

    
def get_sheet_data(sheet, row_start):
    data = []
    for row in range(3, sheet.nrows):
        managers = zip(sheet.cell_value(rowx=row, colx=45).split("\n"),
                       sheet.cell_value(rowx=row, colx=44).split("\n"))
        leads = []
        parts = []
        for m in managers:
            leads.append(m[0]) if is_lead(m) else parts.append(m[0])
            
        data.append(Loan(row_start + row - 3,
                         sheet.cell_value(rowx=row, colx=15),
                         leads, parts,
                         sheet.cell_value(rowx=row, colx=0)))
    return data


def get_loan_sheet_by_index(n):
    return get_sheet_data(xlrd.open_workbook(loans_sheets_paths[n]).sheet_by_index(0), 0)

        
def get_loans_data(n = num_loans):
    data = []
    i = 0
    num = 0
    while i < n:
        data += get_sheet_data(xlrd.open_workbook(loans_sheets_paths[i]).sheet_by_index(0), num)
        num = len(data)
        i += 1
        print("Read sheet " + str(i) + " of " + str(n))
    return array(data)


def get_acquisitions_data():
    sheet = xlrd.open_workbook(acquisitions_path).sheet_by_index(1)
    data = []
    for row in range(1, sheet.nrows):
        if row % 1000 == 0:
            print("Read acquisition " + str(row) + " of " + str(sheet.nrows))
        data.append(Acquisition(sheet.cell_value(rowx=row, colx=1),
                                sheet.cell_value(rowx=row, colx=2),
                                sheet.cell_value(rowx=row, colx=4),
                                sheet.cell_value(rowx=row, colx=13),
                                sheet.cell_value(rowx=row, colx=7)))
    return array(data)


def get_all_banks():
    with open("banks.txt") as f:
        banks = f.read().split("\n")
    return [Bank(b) for b in banks]


def compare_all_banks():
    banks = get_all_banks()
    comparison_matrix = defaultdict(dict)
    for (i, b1) in enumerate(banks):
        if i % 50 == 0:
            print("Bank " + str(i) + " of " + str(len(banks)))
            
        for b2 in banks[:len(banks) - i]:
            score = b1.similarity(b2)
            if score > 0:
                comparison_matrix[b1.name][b2.name] = score
            
    return comparison_matrix


def make_comparison_sheet(comparisons):
    wb = openpyxl.Workbook()
    wb.name = "Names comparison"
    sheet = wb.active
    sheet.title = "Names comparison"

    i = 0
    for (j, b1) in enumerate(comparisons.keys()):
        if j % 500 == 0:
            print(str(j) + " of " + str(len(comparisons)))
        for b2 in comparisons[b1].keys():
            if comparisons[b1][b2] > 0.8:
                i += 1
                sheet.cell(row = i, column = 1).value = b1
                sheet.cell(row = i, column = 2).value = b2
                sheet.cell(row = i, column = 3).value = comparisons[b1][b2]

    print("Saving")
    wb.save("Names.xlsx")
    

def lookup_match(b1, b2, cm):
    try:
        return cm[b1][b2]
    except KeyError:
        try:
            return cm[b2][b1]
        except KeyError:
                return 0


def find_matches(loans, acquisitions, comparisons):
    return [loan.find_matches(acquisitions, comparisons) for loan in loans]


def get_all_matches(acquisitions, comparisons):
    matches = []
    for i in range(len(loans_sheets_paths)):
        matches += find_matches(get_loan_sheet_by_index(i), acquisitions, comparisons)
        print("Matches for sheet " + str(i) + " of " + str(len(loans_sheets_paths)))
    return matches


def remove_duplicates(l):
    seen = []
    for x in l:
        if x not in seen:
            seen.append(x)
    return seen


def make_sheet(loans, acquisitions, matches):
    wb = openpyxl.Workbook()
    wb.name = "Loans data"
    sheet = wb.active
    sheet.title = "Loans data"

    max_leads = max(len(l.leads) for l in loans)
    max_parts = max(len(l.parts) for l in loans)

    sheet.cell(row = 1, column = 1).value = "ID"
    sheet.cell(row = 1, column = 2).value = "DATE"

    for i in range(max_leads):
        sheet.cell(row = 1, column = i + 3).value = "Lead " + str(i + 1)

    for i in range(max_parts):
        sheet.cell(row = 1, column = i + 3 + max_leads).value = "Part " + str(i + 1)

    seen = []
    normalised_matches = remove_duplicates([m[0] for m in matches if m != [[],[]]])
    #print(normalised_matches)
    for (i, m) in enumerate(normalised_matches):
        #print(m)
        #print(normalised_matches)
        if m and m[0] != []:
            sheet.cell(row = 1, column = i + 3 + max_leads + max_parts).value = "Lead" + str(m[0][0]) + "Part" + str(m[0][1])
        elif m and m[1] != []:
            sheet.cell(row = 1, column = i + 3 + max_leads + max_parts).value = "Part" + str(m[1][0][0]) + "Lead" + str([1][0][1])

    for (y, loan) in enumerate(loans):
        if (y % 100 == 0):
            print(str(y) + " of " + str(len(loans)))
            
        match_list = matches[y]
        sheet.cell(row = y + 2, column = 1).value = loan.num
        sheet.cell(row = y + 2, column = 2).value = loan.date

        for (i, lead) in enumerate(loan.leads):
            sheet.cell(row = y + 2, column = i + 3).value = lead.name

        for (i, part) in enumerate(loan.parts):
            sheet.cell(row = y + 2, column = i + 3 + max_leads).value = part.name

        for match in match_list:
            if match != []:
                #print(match)
                sheet.cell(row = y + 2, column = i + 3 + max_leads + max_parts + normalised_matches.index(match)).value = match[0][2]

    wb.save("Loans.xlsx")
