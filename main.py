import locale
import os
import copy
import logging
import re
import json
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import jdatetime
from datetime import timedelta

## Check & get data from tgju.org for currency rate changes
# https://www.tgju.org/?act=archive-tool&noview=&client=ajax&v=200&name=price_dollar_rl&year=1393&month=1&day=1

CONFIG = {
    'logging-level': logging.ERROR,
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
    'results-path': 'results',
    'resume-file': 'last_crawled_date.txt',
    'all-data-file-name': 'all_data.csv',
    'url-prefix': 'https://www.tgju.org/',
    'starting-date': '1393-01-01',
    'ending-date': '1400-12-29',
    'min-starting-date': '1393-01-01',
    'max-ending-date': '1400-12-29',
    'backoff-factor': 1,
    'retry-times': 3,
    'default-timeout': 30,
    'kill-flag': True,
    'output-data-format': 'csv',
}


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = CONFIG['default-timeout']
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class TgjuCurrencyPriceCrawler:
    def __init__(self, starting_date, ending_date, resume_file,
                 user_agent, results_path, retry_times,
                 backoff_factor, default_timeout, all_data_file_name):

        locale.setlocale(locale.LC_ALL, "fa_IR")
        jdatetime.set_locale('fa_IR')
        self.all_data_file_abs_path = ""
        self.results_path = results_path
        self.out_data_format = CONFIG['output-data-format']
        self.min_starting_date = jdatetime.datetime.strptime(CONFIG['min-starting-date'], '%Y-%m-%d')
        self.max_ending_date = jdatetime.datetime.strptime(CONFIG['max-ending-date'], '%Y-%m-%d')
        self.resume_file = CONFIG['resume-file']
        self.resume_file_path = ""
        self.api_call_url = CONFIG['url-prefix']

        self.user_agent = user_agent
        self.retry_times = retry_times
        self.backoff_factor = backoff_factor
        self.default_timeout = default_timeout
        self.all_data_file_name = all_data_file_name

        self.starting_date = starting_date
        self.ending_date = ending_date
        self.date_counter = None

        # for describing the arguments, check this link
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        # the formula:  {backoff factor} * (2 ** ({number of total retries} - 1))
        self.retry_strategy = Retry(
            total=self.retry_times,
            backoff_factor=self.backoff_factor,
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            status_forcelist=[429, 500, 502, 503, 504]
        )

        self.http = requests.Session()
        # we could override the default timeout in adapter level, or per request level
        adapter = TimeoutHTTPAdapter(timeout=self.default_timeout, max_retries=self.retry_strategy)
        self.http.mount('http://', adapter)
        self.http.mount('https://', adapter)
        self.http.headers.update({
            'User-Agent': self.user_agent,
        })

        #
        # {"id": 4338529, "item_id": 137203, "name": "price_dollar_rl", "price": 30170, "high": 30200, "low": 30100,
        #  "open": 30100, "time": "2014-03-20 00:00:00", "updated_at": "2014-03-20 12:00:00"}

        self.columns = [
            'date',
            'day',
            'off',
            'id',
            'item_id',
            'name',
            'price',
            'high',
            'low',
            'open',
            'time',
            'updated_at',
        ]

        self.all_data = pd.DataFrame(columns=self.columns)

    def load_resume_data(self):
        base_path = os.path.dirname(__file__)
        self.resume_file_path = os.path.join(base_path, self.results_path, self.resume_file)
        try:
            with open(self.resume_file_path, 'r') as file:
                self.date_counter = jdatetime.datetime.strptime(file.read().rstrip(), '%Y-%m-%d')
        except IOError:
            print("Resume file '%s' does not exist. Set counter to starting_date." % self.resume_file_path)
            self.date_counter = self.starting_date

    def save_resume_data(self, date_counter):
        base_path = os.path.dirname(__file__)
        self.resume_file_path = os.path.join(base_path, self.results_path, self.resume_file)
        try:
            with open(self.resume_file_path, 'w') as file:
                file.write(date_counter.strftime('%Y-%m-%d'))
        except IOError:
            print("Can not write to '%s' resume file." % self.resume_file_path)

    def load_data(self):
        base_path = os.path.dirname(__file__)
        self.all_data_file_abs_path = os.path.join(base_path, self.results_path, self.all_data_file_name)
        try:
            self.all_data = pd.read_csv(self.all_data_file_abs_path, encoding='utf-8', dtype=object)
        except IOError:
            self.all_data = pd.DataFrame(columns=self.columns)
        finally:
            self.all_data = pd.DataFrame(columns=self.columns)

    def start(self, starting_date=None, ending_date=None):
        if starting_date is not None:
            self.starting_date = jdatetime.datetime.strptime(starting_date, '%Y-%m-%d')
        else:
            self.starting_date = self.min_starting_date
        if ending_date is not None:
            self.ending_date = jdatetime.datetime.strptime(ending_date, '%Y-%m-%d')
        else:
            self.ending_date = self.max_ending_date

        if self.starting_date > self.ending_date:
            raise SystemExit('Error: The starting_date cant not be greater than ending_date')

        if self.starting_date < self.min_starting_date:
            raise SystemExit('Error: The starting_date cant not be less than min_starting_date')

        if self.ending_date > self.max_ending_date:
            raise SystemExit('Error: The starting_date cant not be greater than max_ending_date')

        self.load_resume_data()
        self.load_data()

        delta = self.ending_date - self.date_counter
        remaining_items = delta.days
        item_cnt = 0
        while self.date_counter <= self.ending_date:
            print("Crawling date = %s" % self.date_counter.strftime("%Y-%m-%d"))

            # setting resuming point
            self.save_resume_data(date_counter=self.date_counter)

            # https://www.tgju.org/?act=archive-tool&noview=&client=ajax&v=200&name=price_dollar_rl&year=1393&month=1&day=1
            params = {'act': 'archive-tool', 'noview': '', 'client': 'ajax', 'v': 200, 'name': 'price_dollar_rl',
                      'year': self.date_counter.year, 'month': self.date_counter.month, 'day': self.date_counter.day}
            response = self.http.get(url=self.api_call_url, params=params).json()
            data_row = pd.DataFrame([response], columns=self.columns)
            data_row['date'] = self.date_counter.strftime('%Y-%m-%d')
            data_row['off'] = 0
            data_row['day'] = self.date_counter.strftime('%a')
            # append to main dataframe
            self.all_data = pd.concat([self.all_data, data_row])

            # save list page data
            self.all_data.to_csv(path_or_buf=self.all_data_file_abs_path, encoding='utf-8', index=False)

            item_cnt += 1
            self.date_counter = self.date_counter + timedelta(days=1)
            print("%d/%d Done!" % (item_cnt, remaining_items))

        self.all_data.drop_duplicates(ignore_index=True)


def main():
    logging.basicConfig(level=CONFIG['logging-level'])
    dgc = TgjuCurrencyPriceCrawler(
        starting_date=CONFIG['starting-date'],
        ending_date=CONFIG['ending-date'],
        resume_file=CONFIG['resume-file'],
        user_agent=CONFIG['user-agent'],
        results_path=CONFIG['results-path'],
        retry_times=CONFIG['retry-times'],
        backoff_factor=CONFIG['backoff-factor'],
        default_timeout=CONFIG['default-timeout'],
        all_data_file_name=CONFIG['all-data-file-name'],
    )
    dgc.start()

    print("Done!")


if __name__ == '__main__':
    main()
