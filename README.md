### TSX Company Directory

A tool that scrapes the Toronto Stock Exchange (TSX) to create a directory of:

1. Company ticker symbols
2. Company names
3. Industry classifications

Note: This tool only lists companies. ETFs (Exchange Traded Funds) are not included in the directory.

The information is stored in an SQLite database and can be dumped to JSON with the --dump option.

### Installation

1. Clone this repository
2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

### Usage

Scrape company data:

```bash
./pleco.py --all
```

Output JSON list of all companies:

```bash
./pleco.py --dump
```

Example output:

```json
[
  {
    "symbol": "TSE:AW",
    "name": "A & W Food Services of Canada Inc.",
    "industry": "N/A"
  },
  {
    "symbol": "TSE:AAB",
    "name": "Aberdeen International Inc.",
    "industry": "Finance"
  }
]
```
