### Stock Scraper for Canadian Financial Data
Scrapes several online sources to get:

1. A list of symbols and company names on the Toronto Stock Exchange
2. The industries of the companies
3. Current prices of the stocks
4. Quarterly and annual RPS and revenue for the past year.

The information is then stored in an SQLite database.

Usage:
Scrape the company data
<code>
    ./pleco.py --all
</code>

Output a table of the most growing companys:
<code>
    ./pleco.py --process
</code>
