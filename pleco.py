#!/usr/bin/env python3
import os
import sys
import re
import sqlite3
from bs4 import BeautifulSoup
import urllib.request
import hashlib
import time
import json

# This program will first download a list of stocks from the TSX. Then, for
# each stock, it grabs company information, including company name and
# quarterly EPS and revenue data, and adds it to an SQLITE database.

# All web pages downloaded are stored in a folder called "cache", so they will
# never need to be downloaded again. To get fresh data from the web, you will
# have to delete the cache folder.

# Here is the schema for the SQL database. Note that dates are recorded as the
# number of seconds since January 1, 1970
SCHEMA = """
CREATE TABLE COMPANIES (
    symbol TEXT PRIMARY KEY,
    company TEXT,
    industry TEXT
);

CREATE TABLE PRICES (
    symbol TEXT,
    date INTEGER,
    price INTEGER
);


CREATE TABLE FINANCIALS (
    symbol TEXT,
    type TEXT,
    date TEXT,
    value INTEGER
);

"""

DATABASE_NAME = "pleco.db"
CACHE_FOLDER = "cache"

class Database:
    def __init__(self):
        create = not os.path.exists( DATABASE_NAME )

        self.conn = sqlite3.connect( DATABASE_NAME, timeout=99.0 )
        if create:
            c = self.conn.cursor()
            c.executescript( SCHEMA )

    def addCompany( self, symbol, company, industry ):
        c = self.conn.cursor()
        c.execute( "DELETE FROM COMPANIES WHERE symbol=?", (symbol,));
        c.execute( "INSERT INTO COMPANIES values ( ?, ?, ? )", 
                (symbol, company, industry ) )
        self.conn.commit()

    def getCompanies(self):
        c = self.conn.cursor()
        c.execute( "SELECT * FROM COMPANIES" )
        return c.fetchall();

    def setPrice(self, symbol, date, price):
        c = self.conn.cursor()
        c.execute( "INSERT INTO PRICES VALUES (?, ?, ?)",
                ( symbol, date, price ) )
        self.conn.commit()

    def getPrice(self, symbol ):
        c = self.conn.cursor()
        c.execute( "SELECT price FROM PRICES WHERE symbol=? ORDER BY DATE DESC",
                ( symbol, ) )
        return c.fetchone()[0]

    def getPrice(self, symbol):
        c = self.conn.cursor()
        c.execute( "SELECT price FROM PRICES ORDER BY DATE DESC" )
        return c.fetchone()[0]

    def setFinancials( self, symbol, type, date, value ):
        c = self.conn.cursor()
        c.execute("DELETE FROM FINANCIALS WHERE symbol=? AND type=? and date=?",
                (symbol, type, date))
        c.execute("INSERT INTO FINANCIALS VALUES (?, ?, ?, ?)",
                ( symbol, type, date, value ) )
        self.conn.commit()

    def getFinancials( self, symbol, type ):
        c = self.conn.cursor()
        c.execute( "SELECT * FROM FINANCIALS WHERE symbol=? AND type=? ORDER BY DATE DESC",
                (symbol, type))
        return c.fetchall()

    def getEverything( self ):
        c = self.conn.cursor()
        c.execute( """
                SELECT COMPANIES.symbol, company, industry, type, value, price from
                COMPANIES, PRICES, FINANCIALS WHERE
                COMPANIES.symbol = PRICES.symbol AND PRICES.symbol =
                FINANCIALS.symbol""")

        return c.fetchall()

# This class will fetch a web page from the WWW. However, if the web page
# exists in the cache, it will instead use the cached version.
class PageCache:
    def __init__(self):
        if not os.path.exists( CACHE_FOLDER ):
            os.mkdir( CACHE_FOLDER )

    def get( self, url, fname = None ):
        if fname == None:
            fname = hashlib.sha1(url.encode('utf-8')).hexdigest()
        fname = os.path.join( CACHE_FOLDER, fname )

        if os.path.exists( fname ):
            return open( fname, "rt" ).read()
        else:
            print("Retrieve %s" % url)
            f = urllib.request.urlopen(url)
            content = f.read().decode('utf-8')
            f.close()

            f = open( fname, "w" );
            f.write( content );
            f.close()

            return content

class EmptyClass: pass

# The Pleco class contains logic for scraping the stock information from the
# internet.
class Pleco:
    def __init__(self):
        self.db = Database()
        self.webCache = PageCache()

    # This function will, given a stock symbol, scrape the industry from
    # the global and mail. It returns it as a string.
    def scrapeIndustryForSymbol( self, symbol ):
        symbol = symbol.upper()
        if symbol.startswith("TSE:"):
            symbol = symbol[4:]
        symbol = symbol.replace(".", "-")

        # lookup file, otherwise retrieve the url
        url = f"https://www.theglobeandmail.com/investing/markets/stocks/{symbol}-T/profile/"
        page = self.webCache.get( url )
        
        soup = BeautifulSoup(page, 'html.parser')
        industry_element = soup.find('barchart-field', {"name": "industryGroup"})

        if industry_element is None:
            print(f"Warning: Cannot find industry in {url}")
            return "N/A"

        return industry_element.get('value')

    # This function will, given a stock symbol, scrape the company name from
    # Google Finance. It returns it as a string.
    def scrapeCompanyNameForSymbol( self, symbol ):
        url = "http://www.google.com/finance?q=%s&fstype=ii" % symbol.upper()
        page = self.webCache.get( url )

        expr = re.compile(r"""Financial Statements for (.*?) - Google Finance""")
        m = expr.search(page)
        if m:
            return BeautifulSoup(m.group(1), 'html.parser').contents[0].string
        else:
            return None

    # This function will return a list of all of the stock symbols on the TSX,
    # scraped from the TSX web page.
    def scrapeCompanies( self ):
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        found = {}

        for s in letters:
            url = f"https://www.tsx.com/json/company-directory/search/tsx/{s}"
            page = self.webCache.get(url)
            
            try:
                data = json.loads(page)
                for company in data.get('results', []):
                    symbol = "TSE:" + company['symbol']
                    if symbol in found: 
                        continue
                        
                    found[symbol] = 1
                    name = company['name']
                    industry = self.scrapeIndustryForSymbol(symbol)
                    
                    if name and industry:
                        print(f"Found {name} ({symbol}) - {industry}")
                        self.db.addCompany(symbol, name, industry)
                        
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON for letter {s}: {e}", file=sys.stderr)

    # Assume the database already has the companies table filled in. This
    # function will get the current price of every company that we know about
    # and store it in the prices table, along with the date. 
    def scrapePrices( self ):
        date = int(time.time())
        
        # Given a list of symbols, we get the prices from YAHOO finance and
        # insert them into the PRICES table of the database.
        def getPrices(stocks, list):
            prices = requestYahooPrices( convertToYahooFormat( list ) )

            for i in range(len(prices)):
                self.db.setPrice( list[i], date, prices[i] )
                print("%s = $%.2f" % (list[i], float(prices[i]) / 1000))

        # Given a stock symbol which may be in google finance format, we
        # convert them to yahoo format (eg, ending in .to)
        def convertToYahooFormat( list ):
            # convert from google to yahoo format.
            ret = []
            for symbol in list:
                symbol = symbol[4:] # remove tse:
                symbol = symbol.lower().replace('.', '-') + ".to"
                ret.append( symbol )

            return ret

        # Given a list of stock symbols, we request the current prices from
        # Yahoo and return them as a list. The prices are returned in the same
        # order as the requested symbols so you can match them up.
        def requestYahooPrices(symbols):
            # form HTTP request
            url = "http://finance.yahoo.com/d/quotes.csv?s=%s&f=l1&e=.csv" % \
                ( ",".join(symbols) )
            prices = []

            content = self.webCache.get(url)

            # for each line,
            for line in content.split("\n"):
                line = line.strip()
                if line == "": continue
                prices.append(int(float(line) * 1000))

            # return the list.
            return prices

        stocks = {}
        date = int(time.time())
        for company in self.db.getCompanies():
            stocks[company[0]] = 0

        # for each chunk of 64 stocks,
        array = []
        for key in stocks.keys():
            array.append( key )
            if len( array ) == 64:
                getPrices( stocks, array )
                array = []

        if len( array ) > 0:
            getPrices( stocks, array )
            array = []

    # Scrape the financial information from the quarterly reports of all
    # companies and store in the database.
    def scrapeFinancials( self ):
        for company in self.db.getCompanies():
            self.scrapeFinancialsForSymbol( company[0] )

    # Scrape the financial information from the quarterly reports of a single
    # company and store in the database.
    def scrapeFinancialsForSymbol( self, symbol ):
        date = int(time.time())

        def checkPresence( page, pattern ):
            for line in page:
                if line.find(pattern) != -1:
                    return True

            return False

        def extractRow( soup, text ):
            def byname(tag):
                return str(tag.string).rstrip() == text and tag.name == 'td'

            tag = soup.find(byname)
            contents = []
            while tag:
                tag = tag.findNextSibling('td')
                if tag == None: break
                contents.append(str(tag.find(text=True)))
            return moneyToNumber(contents)

        def moneyToNumber( arr ):
            ret = []
            for a in arr:
                if a == '-':
                    ret.append(0)
                else:
                    ret.append(int(float(a.replace(",", "")) * 1000 ))

            return ret

        def extractDates( lines ):
            values = []
            expr = re.compile(r"""(\d{4}-\d{2}-\d{2})""")
            for line in lines:
                m = expr.search(line)
                if m:
                    values.append( m.group(0) )
                else:
                    values.append("")

            return values

        def findLinesLike( page, pattern ):
            lines = []
            skipped = -1
            pattern = re.compile(pattern)
            for line in page:
                if pattern.search(line):
                    lines.append( line )
                    skipped = 0
                elif skipped >= 0:
                    skipped += 1
                    if skipped >= 5:
                        break
            return lines

        print("Scraping financials for %s" % symbol)

        # retrieve the web page
        url = "http://www.google.com/finance?q=%s&fstype=ii" % symbol
        page = self.webCache.get( url )
        soup = BeautifulSoup(page, 'html.parser')
        page = page.split('\n')
        quarterlyPage = soup.find( "div", { "id" : "incinterimdiv" } )
        annualPage = soup.find( "div", { "id" : "incannualdiv" } )

        qstr = str(quarterlyPage).split('\n')
        astr = str(annualPage).split('\n')

        # Look for "In Millions of". If not there, error!
        if not checkPresence( page, "In Millions of" ):
            print("While processing %s could not find 'In Millions of' at %s" % (symbol, url), file=sys.stderr)
            return False

        # Set multiplier to 1000000
        multiplier = 1000000

        # build array of all lines like "3 months Ending"
        quarterlyDates = extractDates(findLinesLike( qstr, r"""\d+ (months|weeks) ending""" ))

        # Build array of all lines like "12 months Ending"
        annualDates = extractDates(findLinesLike( astr, r"""\d+ (months|weeks) ending""" ))

        # Look for td containing "Total Revenue"
        # Extract all td elements in siblings that contain only a number

        # Build table for revenue
        quarterlyRevenue = extractRow( quarterlyPage, "Revenue" )
        annualRevenue = extractRow( annualPage, "Revenue" )

        # Build table for ";Diluted EPS Normalized EPS&"
        quarterlyEPS = extractRow( quarterlyPage, "Diluted Normalized EPS" )
        annualEPS = extractRow( annualPage, "Diluted Normalized EPS" )

        for i in range( len(quarterlyRevenue) ):
            self.db.setFinancials( symbol, "QuarterlyRevenue", quarterlyDates[i],
                    quarterlyRevenue[i] * multiplier )
            self.db.setFinancials( symbol, "QuarterlyEPS", quarterlyDates[i],
                    quarterlyEPS[i] )

        for i in range( len(annualRevenue) ):
            self.db.setFinancials( symbol, "AnnualRevenue", annualDates[i],
                    annualRevenue[i] * multiplier )
            self.db.setFinancials( symbol, "AnnualEPS", annualDates[i],
                    annualEPS[i] )

    def addProjected( self, symbol, type ):
        financials = self.db.getFinancials( symbol, "Quarterly%s" % type )
        if len(financials) < 4:
            return

        projected = financials[0][3] + financials[1][3] + financials[2][3] + \
                    financials[3][3]

        self.db.setFinancials( symbol, "Projected%s" % type, 0, projected )

    def addAverageGrowth( self, symbol, type ):
        financials = self.db.getFinancials( symbol, "Annual%s" % type )
        avgGrowth = 0.0
        if len(financials) > 1:
            projected = self.db.getFinancials( symbol, "Projected%s" % type )
            financials.extend( projected )
            financials.reverse()
            first = financials[0][3]
            count = 0
            for val in financials:
                if first > 0:
                    growth = float((val[3] - first)) / first
                    avgGrowth += growth
                    count += 1
                else:
                    avgGrowth = 0.0
                    count = 0
                first = val[3]

            if count < 2:
                avgGrowth = 0.0
            else:
                avgGrowth /= count
        
        self.db.setFinancials( symbol, "Average%sGrowth" % type, 0, 
                round( avgGrowth * 100 ) )

    def addYearsOfGrowth( self, symbol, type ):
        financials = self.db.getFinancials( symbol, "Annual%s" % type )
        count = 0
        if len(financials) > 0:
            last = financials[0]
            for line in financials[1:]:
                if line[3] < last:
                    count += 1
                else:
                    break

        self.db.setFinancials( symbol, "YearsOf%sGrowth" % type, 0, count )

    def addPE( self, symbol ):
        price = self.db.getPrice( symbol )
        financials = self.db.getFinancials( symbol, "ProjectedEPS" )
        if len(financials) == 0:
            return

        earnings = financials[0][3]
        if earnings > 0:
            pe = round(float(price)/float(earnings) * 10)
        else:
            pe = 0

        self.db.setFinancials( symbol, "PE", 0, pe );

    def addExtraInfo( self ):
        for company in self.db.getCompanies():
            symbol = company[0]
            print("Processing %s...    \r" % symbol, end='')
            sys.stdout.flush()
            self.addProjected(symbol, "EPS")
            self.addProjected(symbol, "Revenue")
            self.addAverageGrowth( symbol, "EPS" )
            self.addAverageGrowth( symbol, "Revenue" )
            self.addYearsOfGrowth( symbol, "EPS" )
            self.addYearsOfGrowth( symbol, "Revenue" )
            self.addPE( symbol )

        print()

    def dump(self):
        companies = []
        for company in self.db.getCompanies():
            companies.append({
                "symbol": company[0],
                "name": company[1],
                "industry": company[2]
            })
        print(json.dumps(companies, indent=2))

    def process(self):
        stocks = {}
        for record in self.db.getEverything():
            symbol = record[0]
            company = record[1]
            industry = record[2]
            type = record[3]
            value = record[4]
            price = record[5]
            if symbol not in stocks:
                stock = { "symbol": symbol, 
                    "price": price, 
                    "company": company,
                    "industry": industry}
                stocks[symbol] = stock
            else:
                stock = stocks[symbol]

            stock[type] = value

        stocks = filter( self.filt, stocks.values() )

        stocks.sort( key = lambda stock: stock["AverageRevenueGrowth"] )
        self.printTable(stocks)

    def filt(self, stock):
        return \
            stock["YearsOfRevenueGrowth"] >= 1 and \
            stock["YearsOfEPSGrowth"] >= 1 and \
            stock["AverageRevenueGrowth"] >= 5 and \
            stock["AverageEPSGrowth"] >= 5 and \
            "PE" in stock and \
            stock["PE"] >= 0 and \
            stock["PE"] <= 50 \
            and stock["ProjectedEPS"] > 0 \
            and stock["industry"].find("Oil") == -1 \
            and stock["industry"].find("Mining") == -1 \
            and stock["industry"].find("Metals") == -1 \
            and stock["industry"].find("Diversified") == -1 \
            and stock["industry"].find("Forestry") == -1 

    def printTable(self, stocks):
        print("symbol, AverageRevenueGrowth, YearsOfRevenueGrowth, AverageEPSGrowth, YearsOfEPSGrowth, PE, Company")
        for stock in stocks:
            print(stock["symbol"].ljust(13), end=' ')
            print(str(stock["AverageRevenueGrowth"]).ljust(5), end=' ')
            print(str(stock["YearsOfRevenueGrowth"]).ljust(3), end=' ')
            print(str(stock["AverageEPSGrowth"]).ljust(5), end=' ')
            print(str(stock["YearsOfEPSGrowth"]).ljust(3), end=' ')
            print(str(stock["PE"]).ljust(5), end=' ')
            print(stock["company"])

    def run(self):
        for i in range(1, len(sys.argv)):
            if sys.argv[i] == "--companies":
                self.scrapeCompanies()
            elif sys.argv[i] == "--prices":
                self.scrapePrices()
            elif sys.argv[i] == "--financials":
                self.scrapeFinancials()
            elif sys.argv[i] == '--extra':
                self.addExtraInfo()
            elif sys.argv[i] == "--all":
                self.scrapeCompanies()
                self.scrapeFinancials()
                self.scrapePrices()
                self.addExtraInfo()
            elif sys.argv[i] == "--test": 
                self.addPE("tse:g")
            elif sys.argv[i] == "--process":
                self.process()
            elif sys.argv[i] == "--dump":
                self.dump()


Pleco().run()

