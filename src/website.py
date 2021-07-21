'''
this file contains an implementation of a crude webscraper i put together that
is used to get the data from the web

If the crawler is a bottleneck, consider using Twisted async and send request 
in parallel or use something like scrapy. 


'''




import time
import urllib.request
from bs4 import BeautifulSoup
import re
import os

'''
Contains information about the website structure
'''
class Website:

    def __init__(self, name, url, type):
        self.name = name
        self.url = url
        self.type = type





'''
An instance of a web crawler
'''
class Crawler:

    def __init__(self, website, search_pattern, outfile, sleep_time=5,):
        self.website = website
        self.search_pattern = search_pattern
        self.outfile = outfile
        self.sleep_time = sleep_time
        self.urls = set()
        self.pages = set()
        self.page_counter = 0
        self.saved_counter = 0


    def get(self, pageUrl, cut=None, url_pattern=None):

        # adjustment that must be made in order for the scraper to work properly with absolute paths
        if self.website.type == 'A':
            pageUrl = self.website.url

        start = time.time()
        self.scrape(pageUrl, cut, url_pattern)
        end = time.time()
        print("Total time elapsed: {} sec".format(round(end - start)))
        print("Scraped {} files crawling over {} pages".format(self.saved_counter, self.page_counter))



    # Scrapes all of the target URLs off of the website
    def scrape(self, pageUrl, cut, url_pattern):

        # manage stats
        self.page_counter += 1
        if self.page_counter % 100 == 0:
            time.sleep(self.sleep_time)


        try:
            # append the relative address to the base address and GET
            if self.website.type == 'R':
                # print("{}{}".format(self.website.url, pageUrl))
                html = urllib.request.urlopen("{}{}".format(self.website.url, pageUrl))

            else:
                # print("{}".format(pageUrl))
                html = urllib.request.urlopen("{}".format(pageUrl))


        # this url doesnt exist, will return a 404
        except:
            return

        # update our base address
        # useful for Open data Alberta because their pages are relative to https://open.alberta.ca
        if cut:
            cut_len = -1 * len(cut)
            self.website.url = self.website.url[0:cut_len]
            cut = None

        bs = BeautifulSoup(html, 'lxml')

        # scan the current page for any desired files
        for url in bs.find_all('a', href=re.compile("{}".format(self.search_pattern))):
            if url.attrs['href'] not in self.urls:
                newUrl = url.attrs['href']
                self.urls.add(newUrl)
                self.saved_counter += 1


        if self.website.type == 'R':
            if url_pattern:
                matches = bs.find_all('a', href=re.compile("^{}".format(url_pattern)))

            else:
                matches = bs.find_all('a', href=re.compile("^/"))

        else:
            matches = bs.find_all('a', href=re.compile("^{}".format(self.website.url)))

        # scan for more traversal link
        for link in matches:
            if 'href' in link.attrs:
                if link.attrs['href'] not in self.pages:
                    # this is a new page, add it to the list of sites to be scraped
                    newPage = link.attrs['href']
                    self.pages.add(newPage)
                    self.scrape(newPage, cut, url_pattern)



    # writes all of the scraped URLs to file
    #
    # probably wont use this unless you're sampling some data source
    def write_to_file(self):
        try:
            # remove the old csv url file if there is one
            if os.path.exists(self.outfile):
                os.remove(self.outfile)

            f = open(self.outfile, "a")

        except FileNotFoundError:
            print("The output file could not be found!")

        if "dashboard" in self.website.url:
            for url in self.urls:
                f.write(self.website.url + url + '\n')

        else:

            for url in self.urls:
                f.write(url + '\n')

    def get_urls(self):
        return self.urls



