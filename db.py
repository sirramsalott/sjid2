from os.path import join, isfile, exists
from os import listdir, mkdir
from string import punctuation
from sys import maxsize
from numpy import array
import xlrd, openpyxl, json, pickle
from time import time, sleep
from company_name_similarity import CompanyNameSimilarity
from collections import defaultdict
from datetime import datetime


"""
JOE RAMSAY'S SCRIPT FOR COMPARING BANKS


REQUISITES:

This program assumes the following file structure:

All files are stored in subdirectories of document_home - you will
need to change this variable (defined immediately after this extended
comment) to run on your own machine. You may need to put an 'r' on the
beginning of the string for it to run on Windows (eg
r"C:/blah/Documents...")

Within document_home, the acquisitions database is stored in a folder
"M&As Data", and the loans databases are stored in "Syndicated Loan
Data", with subdirectories "UK", "US" and "GlobalminusUSUK". I think
that this is how the files are structured on your machine, but you may
need to juggle some of this if not.

You will need Python 3, as well as some non-standard modules. xlrd and
openpyxl can both be downloaded from pypi.python.org, where there are
better guides to setting them up than I can provide. numpy can be
installed by downloading scipy, also from pypi, but there may be
better ways to do it on Windows. The standard version of
CompanyNameSimilarity will not work with this program - I have
included my modified version of this library, as well as another file
that it uses. This needs to go in the same folder as this file. I
think that's all the non-standard modules, but let me know if there
are any others - Python will warn of ImportError or something similar
when you load this file if there's anything missing

There are a few data files that also need to be in the same directory
as this file. wc.txt, which is a list of place names, and banks.txt,
which is a list of all the banks mentioned in either database.

Basically, just put all of the files I email you in a folder with the
databases and everything should work.


RUNNING:

To run the script, load this file into a Python interpreter. Wait 2 or
3 seconds for all the data to load, then run the function run(). You
can vary the similarity threshold by supplying it with an argument (eg
run(threshold=1.4)). I found a threshold 0.9 worked reasonably well,
but if you start finding matches which you think are not correct, you
can raise it. Similarly, if you think some matches may be missed, you
can lower it, but I found that at around 0.85 and below you begin to
get matches which are clearly wrong.

This gives a file Loans.xlsx.

To give IDs to all syndicated loans, run add_ids_to_loans(). This
gives them an ID which corresponds to the ID in Loans.xlsx. New files
will be put in the subdirectory "Syndicated Loan Data modified".

Both of these scripts take a reasonably long time, and will use up all
of your memory. Neither should take more than about an hour, but it's
probably best to set the program runnning overnight, or at least over
a long coffee break. I sometimes found that it was best to restart my
computer after the program was finished, but a more powerful computer
than mine may not have this problem.
"""


# SET UP GLOBAL VARIABLES
document_home = "/home/joe/sajid" #<--CHANGE THIS TO SUIT YOUR SYSTEM
acquisitions_path = join(document_home, "M&As Data/Zephyr_Export_Updated.xls")
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
             'georgia', 'credit', 'austria', 'ubs', 'raiffeisen',
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
            if x in places:
                locations.add(x)
            else:
                tokens += x + " "
        return (tokens[:-1], locations)

    
    def similarity(self, bank):
        if self.location != bank.location:
            return 0
        score = cm.match_score(self.n_name, bank.n_name, self.name_set, bank.name_set)
        return score
        
    
class Loan(object):
    def __init__(self, num, date, leads, parts, borrower):
        self.num = num
        self.date = datetime.strptime(date.replace("--", "01"), r"%m/%d/%y")
        self.leads = [Bank(l) for l in leads]
        self.parts = [Bank(p) for p in parts]
        self.all_managers = self.leads + self.parts
        self.borrower = borrower

    def __repr__(self):
        return str(self.__dict__)


class Acquisition(object):
    def __init__(self, num, acquiror, target, date, status, datemode):
        self.num = num
        self.acquiror = Bank(acquiror)
        self.target = Bank(target)
        if date == "":
            self.date = False
        else:
            self.date = xlrd.xldate.xldate_as_datetime(float(date), datemode)
        self.status = status
        
    def __repr__(self):
        return str(self.__dict__)


def is_lead(manager):
    return manager[1] in {"CO-MANAGER", "LEAD MANAGER", "CO-LEAD MANAGER", "BOOKRUNNER"}

    
def get_sheet_data(sheet, row_start):
    'read a sheet of loans into a list of Loan objects'
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
    'Get all loans data, with a possible cap of n for testing'
    print("Reading loans")
    data = []
    i = 0
    num = 0
    while i < n:
        data += get_sheet_data(xlrd.open_workbook(loans_sheets_paths[i]).sheet_by_index(0), num)
        num = len(data)
        i += 1
        print("Read loan " + str(i) + " of " + str(n))
    return array(data)


def get_acquisitions_data():
    'Read in the acquisitions database'
    print("Reading acquisitions")
    book = xlrd.open_workbook(acquisitions_path)
    sheet = book.sheet_by_index(1)
    data = []
    for row in range(1, sheet.nrows):
        if row % 1000 == 0:
            print("Read acquisition " + str(row) + " of " + str(sheet.nrows))
        data.append(Acquisition(sheet.cell_value(rowx=row, colx=1),
                                sheet.cell_value(rowx=row, colx=2),
                                sheet.cell_value(rowx=row, colx=4),
                                sheet.cell_value(rowx=row, colx=13),
                                sheet.cell_value(rowx=row, colx=7), book.datemode))
    return array(data)


def get_all_banks():
    'Read in all the names of banks encountered in the database (banks.txt does not change automatically if you bring in new bank names)'
    with open("banks.txt") as f:
        banks = f.read().split("\n")
    return [Bank(b) for b in banks]


def compare_all_banks(maxbanks=False, threshold=0.0):
    'Create a comparison matrix of bank names for lookup later on'
    print("Creating comparison matrix")
    banks = get_all_banks()
    if maxbanks:
       banks = banks[:maxbanks]
    comparison_matrix = defaultdict(dict)
    for (i, b1) in enumerate(banks):
        if i % 50 == 0:
            print("Compared bank " + str(i) + " of " + str(len(banks)))
            
        for b2 in banks[:len(banks) - i]:
            score = b1.similarity(b2)
            if score > threshold:
                comparison_matrix[b1.name][b2.name] = score
                comparison_matrix[b2.name][b1.name] = score
    return comparison_matrix


def checkComparisons(comparisons):
    for c in comparisons:
        if not isinstance(c, str):
            print([c, c.__class__])


def lookup_match(b1, b2, cm):
    'look up the comparison score for 2 banks in the comparison matrix'
    try:
        return cm[b1][b2]
    except KeyError:
        try:
            return cm[b2][b1]
        except KeyError:
                return 0

def writeAcquisitions(banks1, banks2, date, owners, comparisons, sheet, offset, name1, name2, seenpairs, y):
    for i, bank1 in enumerate(banks1):
        # What other names does this bank go under?
        for cb1 in comparisons[bank1.name]:
            for j, bank2 in enumerate(banks2):
                # What other names does this bank go under?
                for cb2 in comparisons[bank2.name]:
                    # Does bank1 own bank2 (under these names)?
                    try:
                        # bank i bought j in transaction owners[cb1][cb2]
                        match = owners[cb1][cb2]
                    except:
                        pass
                    else:
                        if (match[1] and match[1] > date):
                            if (name1 + str(i), name2 + str(j)) not in seenpairs:
                                seenpairs.append((name1 + str(i), name2 + str(j)))
                                sheet.cell(row = 1, column = offset + seenpairs.index((name1 + str(i), name2 + str(j)))).value = name1 + str(i + 1) + name2 + str(j + 1)

                            sheet.cell(row = y + 2, column = offset + seenpairs.index((name1 + str(i), name2 + str(j)))).value = match[0]


def makeLoanTable(acquisitions, loans, comparisons):
    """
    Make a table of everyone who owns someone else
    and the transaction where they bought them
    """
    print("Creating loans database")
    
    owners = {}
    for a in acquisitions:
        acquiror = a.acquiror.name
        target = a.target.name
        if not acquiror in owners:
            owners[acquiror] = {}
        owners[acquiror][target] = (a.num, a.date)

    """
    Find out how many columns of leads and of parts you're
    going to need for printing it out 
    """
    maxleads = max(len(loan.leads) for loan in loans)
    maxparts = max(len(loan.parts) for loan in loans)
    """
    Now go through all your loans: what you want is to know whether
    any of the leads in this loan are the owners of any of the parts,
    and if so which transaction was involved.

    This would be easy if banks went by their rightful names in the
    list if acquisitions. But they don't. So you have to look at the
    transactions involving people with similar names.
    """

    #Set everything up
    wb = openpyxl.Workbook()
    wb.name = "Loans data"
    sheet = wb.active
    sheet.title = "Loans data"

    sheet.cell(row = 1, column = 1).value = "ID"
    sheet.cell(row = 1, column = 2).value = "DATE"

    for i in range(maxleads):
        sheet.cell(row = 1, column = i + 3).value = "Lead " + str(i + 1)
    for i in range(maxparts):
        sheet.cell(row = 1, column = i + 3 + maxleads).value = "Part " + str(i + 1)

    """
    leadleadseenpairs = []
    leadpartseenpairs = []
    partleadseenpairs = []
    partpartseenpairs = []
       """
    seenpairs = []
    
    for y, loan in enumerate(loans):
        if y % 1000 == 0: print("Created " + str(y) + " of " + str(len(loans)))
        sheet.cell(row = y + 2, column = 1).value = loan.num
        sheet.cell(row = y + 2, column = 2).value = str(loan.date.date())
        
        for j, lead in enumerate(loan.leads):
            sheet.cell(row = y + 2, column = j + 3).value = lead.name
        for j, part in enumerate(loan.parts):
            sheet.cell(row = y + 2, column = j + 3 + maxleads).value = part.name
        writeAcquisitions(loan.leads, loan.leads, loan.date, owners, comparisons, sheet,
                          3 + maxleads + maxparts, "Lead", "Lead", seenpairs, y)
        writeAcquisitions(loan.leads, loan.parts, loan.date, owners, comparisons, sheet,
                          3 + maxleads + maxparts, "Lead", "Part", seenpairs, y)
        writeAcquisitions(loan.parts, loan.leads, loan.date, owners, comparisons, sheet,
                          3 + maxleads + maxparts, "Part", "Lead", seenpairs, y)
        writeAcquisitions(loan.parts, loan.parts, loan.date, owners, comparisons, sheet,
                          3 + maxleads + maxparts, "Part", "Part", seenpairs, y)
    
    wb.save("Loans.xlsx")
        
    
def run(threshold=0.9):
    t1 = int(time())
    loans = get_loans_data()
    t2 = int(time())
    print("Read loans in %d seconds"%(t2 - t1))
    acquisitions = get_acquisitions_data()
    t3 = int(time())
    print("Read acquisitions in %d seconds"%(t3-t2))
    comparisons = compare_all_banks(threshold=threshold)
    t4 = int(time())
    print("Built comparison matrix in %d seconds"%(t4-t3))
    makeLoanTable(acquisitions, loans, comparisons)


def add_ids_to_loans():
    num = 0

    newdir = document_home + "/Syndicated Loan Data modified"
    if not exists(newdir):
        mkdir(newdir)
        mkdir(newdir + "/GlobalminusUSUK")
        mkdir(newdir + "/UK")
        mkdir(newdir + "/US")
    
    for n, p in enumerate(loans_sheets_paths):
        sheet = xlrd.open_workbook(p).sheet_by_index(0)
        wb = openpyxl.Workbook()
        wb.name = "Combined Loans"
        newsheet = wb.active
        newsheet.title = "Combined Loans"

        newsheet.cell(row = 1, column = 1).value = "ID"
        for i in range(sheet.ncols):
            newsheet.cell(row = 1, column = i + 2).value = sheet.cell_value(rowx = 2, colx = i)
        
        for i in range(3, sheet.nrows):
            newsheet.cell(row = i - 1, column = 1).value = num
            
            for j in range(sheet.ncols):
                newsheet.cell(row = i - 1 , column = j + 2).value = sheet.cell_value(rowx = i, colx = j)
            num += 1
            
        pathname = p.replace("Syndicated Loan Data", "Syndicated Loan Data modified") + "x"
        print("Copied database " + str(n + 1) + " of " + str(len(loans_sheets_paths)))
        wb.save(pathname)

