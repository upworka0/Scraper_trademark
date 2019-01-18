# scraper for trade_mark
Python3 CLI to scraper trade_mark number data ,drop them as csv format, finally upload csv to mysql database
<br><br><br>
python3 trade_parse.py --verbose <`file` or `id`> --trademark <filename.txt> (list of trademark numbers) --csv <filename.csv> --json(flag for json) --mysql (flag for mysql) <br>
## Example
<p>python3 trade_parse.py --verbose file --trademark list.txt --csv out.csv --json 0 --mysql 1<br></p>
<p>python3 trade_parse.py --verbose id --trademark 111045 --csv out.csv --json 0 --mysql 0<br></p>
python3 trademarkparse.py --help
