import errno
import linecache
import mimetypes
import os
import signal
import multiprocessing
import sys
from datetime import datetime
import time
from functools import wraps
from urllib.parse import urlparse, urljoin
import concurrent.futures


from func_timeout import func_timeout, FunctionTimedOut
import pandas as pd
import requests
from bs4 import BeautifulSoup
from googletrans import Translator
import goslate
import translate
from validator_collection import checkers

HEADER = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
                       "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}



class Scraper:
    FILE_TYPES = ["download", "pdf", "docx", "doc", "xls", "xlsx", "csv", "upload", "file"]
    CONTENT_TYPES = ["pdf", "word", "excel", "officedocument"]
    FILE_EXTENSIONS = ["pdf", "docx", "doc", "xls", "xlsx" "csv"]

    def __init__(self, seed_url=None, output_dir=None, max_run_time_downloads=1200, max_run_time_requests=20,
                 page_lan=None):
        """

        :param country_urls: dict object of country ID and link
        :param output_dir: where to save data
        """
        self.seed_url = seed_url
        self.output_dir = output_dir
        self.visited_websites_text = None
        self.visited_website_urls = None
        self.time_passed = 0
        self.max_run_time_downloads = max_run_time_downloads
        self.max_run_time_requests = max_run_time_requests
        self.page_lan = page_lan
        self.translate = False
        self.key_words = None
        self.en_key_words = None
        self.country_base_url = None
        self.visit_record = {}
        self.include_any_downloadable = True
        self.start_time = datetime.now()
        self.time_passed = 0

    def get_request_response(self, url=None):
        try:
            response = func_timeout(self.max_run_time_requests, load_url, args=(url,))
            return response
        except FunctionTimedOut:
            print("The requests to URL timed out after {} seconds".format(self.max_run_time_requests))

    def download_content(self, web_link=None):

        try:
            response = self.get_request_response(url=web_link)
            if response.status_code == 200:
                # check if we have arleady visited this website
                if self.check_already_visited(url_text=response.text, url_name=web_link):
                    return

                # update visit log
                self.visit_record[web_link] = response.text

                # check if it has downloadable content and download
                if self.has_downloadable_content(url=web_link, req_response=response):
                    self.download_file(url_link=web_link, req_response_obj=response)

                # otherwise get the links on this page
                return list(set(self.get_relevant_links(target_url=web_link)))
            else:
                full_url = create_full_url_from_part(base_url=self.country_base_url, content_url=web_link)
                if not full_url:
                    return
                return [full_url]
        except Exception as e:
            full_url = create_full_url_from_part(base_url=self.country_base_url, content_url=web_link)
            if not full_url:
                return
            return [full_url]

    def filter_and_download_v1(self, url_links=None):
        """
        Assuming
        :param url_links:
        :param filter_term:
        :return:
        """
        new_relevant_links = []
        for i in url_links:
            time_now = datetime.now()
            diff = (time_now-self.start_time).total_seconds()
            self.time_passed = diff
            if self.time_passed > self.max_run_time_downloads:
                print("Function run more than {} minutes".format(int(self.max_run_time_downloads)))
                return
            print(i)
            result = self.download_content(web_link=i)
            if result:
                new_relevant_links += result
                
        if new_relevant_links:
            return self.filter_and_download_v1(url_links=new_relevant_links)
        print("Completed going through all relevant links for country")
        return

    def filter_and_download_v2(self, url_links=None):
        """
        Assuming
        :param url_links:
        :param filter_term:
        :return:
        """
        new_relevant_links = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.download_content, url): url for url in url_links}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                print(url)
                try:
                    data = future.result()
                    if data:
                        new_relevant_links += data

                except Exception as exc:
                    # print('%r generated an exception: %s' % (url, exc))
                    pass

        if new_relevant_links:
            return self.filter_and_download_v2(url_links=new_relevant_links)
        print("Completed going through all relevant links for country")
        return

    def get_relevant_links(self, target_url=None):

        try:

            links = get_links_from_target_sites(url=target_url, use_header=True)
            if self.translate:
                key_words = translate_key_words(dest_lan=self.page_lan, en_key_words=self.en_key_words)
                if not key_words:
                    key_words = self.en_key_words
            else:
                key_words = self.key_words

            relevant_urls = []
            for k, v in links.items():
                if self.include_any_downloadable:
                    for d in self.FILE_TYPES:
                        if d in k.lower() or d in v.lower():
                            relevant_urls.append(k)
                            break

            for k in relevant_urls:
                del links[k]

            for k, v in links.items():
                for w in key_words:
                    if w in k.lower() or w in v.lower():
                        relevant_urls.append(k)
                        break
        except Exception:
            return

        if self.translate and not relevant_urls:
            return links
        return relevant_urls

    def has_downloadable_content(self, url=None, req_response=None):
        """
        Does the url contain a downloadable resource
        """
        try:

            filename = url.split("/")[-1]
            for e in self.FILE_EXTENSIONS:
                if e in filename:
                    return True

            headers = req_response.headers
            content_type = headers.get("Content-Type")

            for f in self.CONTENT_TYPES:
                if f in content_type:
                    return True

            if 'text' in content_type.lower():
                return False
            if 'html' in content_type.lower():
                return False

            return True
        except Exception:
            return False

    def download_file(self, url_link=None, req_response_obj=None):
        """
        Download and save file if possible
        :param url:
        :param outfolder:
        :return:
        """
        try:
            filename = retrieve_filename_and_extension(response=req_response_obj, url=url_link)
            fpath = os.path.join(self.output_dir, filename)
            open(fpath, 'wb').write(req_response_obj.content)
            return filename
        except Exception:
            return None

    def retrieve_filename_and_extension(self, response=None, url=None):

        try:
            filename = url.split("/")[-1]
            for e in self.FILE_EXTENSIONS:
                if e in filename:
                    return filename

            content_type = response.headers['content-type']
            for c in self.CONTENT_TYPES:
                if c in content_type.lower():
                    extension = mimetypes.guess_extension(content_type)
                    if not extension:
                        filename = url.split("/")[-2] + ".{}".format(c)
                    else:
                        filename = url.split("/")[-1] + extension
                    return filename

            return filename + ".pdf"
        except Exception:
            filename = url.split("/")[-1]
            return filename + ".pdf"

    def check_already_visited(self, url_text=None, url_name=None):

        visit_logs = self.visit_record

        if url_name in visit_logs:
            return True
        for k, v in visit_logs.items():
            if url_text == v:
                return True

    def download(self, starter_url=None, use_func_timer=False):

        try:
            relevant_links = list(set(self.get_relevant_links(target_url=starter_url)))
            if use_func_timer:
                func_timeout(self.max_run_time_downloads, self.filter_and_download_v1, args=(relevant_links,))
            else:
                self.filter_and_download_v1(url_links=relevant_links)

        except FunctionTimedOut:
            print("The downloading went on for {} minutes and timed out".format(self.max_run_time_downloads/60))
        except Exception as e:
            print(e)
            return


def load_url(url=None):

    session = requests.session()
    response = session.get(url, headers=HEADER, allow_redirects=True, verify=False)

    return response


def sanitize_url(url):
    root = "http://" + urlparse(url).hostname
    parsed = list(urlparse(url))

    new_lst = []
    for i in parsed[2:]:
        if i:
            x = i.strip()
            xx = x.replace("//", "/")
            new_lst.append(xx)

    clean_url = urljoin(root, "/".join(new_lst))
    return clean_url


def recursive_download(url=None, output_dir=None):

    try:
        if has_downloadable_content(url):
            download_file(url=url, outfolder=output_dir, use_header=True)
        else:
            more_links = get_links_from_target_sites(url=url)
            for i in more_links:
                return recursive_download(url=i, output_dir=output_dir)

    except Exception as e:
        print("Something went wrong: {}".format(e))


def filter_and_download(url_links=None, download_everything=False, keywords=None,
                        page_lan=None, country_base_url=None,
                        output_dir=None, visit_freqs=None):
    """
    Assuming
    :param url_links:
    :param filter_term:
    :return:
    """
    # TODO: Ensure it doesnt get stuck 
    translate_pg = False
    if page_lan != "en":
        translate_pg = True

    relevant_links = get_relevant_links(key_words=keywords, src_lan=page_lan, links=url_links, translate=translate_pg,
                                        include_any_downloadable=download_everything)
    if not relevant_links:
        return "No relevant links"
    visit_log = visit_freqs
    for i in relevant_links:
        print(i)
        if i in visit_log:
            cnt = visit_log[i]
            visit_log[i] = cnt + 1
        else:
            visit_log[i] = 1

        if visit_log[i] > 3:
            continue
        try:
            if has_downloadable_content(i):
                download_file(url=i, outfolder=output_dir, use_header=True)
                continue
            full_url = create_full_url_from_part(base_url=country_base_url, content_url=i)
            if full_url in relevant_links or i in relevant_links:
                continue

            # check if url is valid b y calling requests
            req = requests.get(full_url, verify=False)

            if req.status_code == 200:
                new_links = get_links_from_target_sites(url=full_url, use_header=True)
                return filter_and_download(url_links=new_links, download_everything=download_everything,
                                           keywords=keywords, page_lan=page_lan, country_base_url=country_base_url,
                                           output_dir=output_dir, visit_freqs=visit_log)
        except Exception as e:
            continue


def avoid_double_slash(path):
    parts = path.split('/')
    not_empties = [part for part in parts if part]
    return '/'.join(not_empties)


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def has_downloadable_content(url):
    """
    Does the url contain a downloadable resource
    """
    try:
        file_extensions = ["pdf", "docx", "doc", "xls", "xlsx" "csv"]
        content_type_list = ["pdf", "word", "excel", "officedocument"]

        filename = url.split("/")[-1]
        for e in file_extensions:
            if e in filename:
                return True

        response = requests.get(url)
        if response.status_code != 200:
            return None
        headers = response.headers
        content_type = headers.get("Content-Type")

        for f in content_type:
            if f in content_type_list:
                return True

        if 'text' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False

        return True
    except Exception:
        return False


def get_weblinks(fpath=None, required=True):
    """
    Gets website links and put them in a data frame
    :param fpath:
    :return:
    """

    df = pd.read_csv(fpath)
    if required:
        df = df[(df.linkAvailable == "Yes") & (df.downloadStatus == "Not started")]

    country_dict = {}
    for index, row in df.iterrows():
        country_code = row["countryCode"]
        country_name = row["DMFCountryName"]
        url = row["link"]
        country_dict[country_code] = {"targetUrl": url, "countryName": country_name, "lan": row["language"]}

    return country_dict


def get_links_from_target_sites(url=None, use_header=False):
    """
    Given the website, gets the links
    :param url:
    :return:
    """
    # TODO: detect webpage language and return it
    try:
        if use_header:
            session = requests.session()
            response = session.get(url, headers=HEADER, timeout=20)

        else:
            response = requests.get(url, verify=False, timeout=20)

        if response.status_code != 200:
            return None
        html = response.text
        bs = BeautifulSoup(html, "html.parser")
        urls = {}
        for a in bs.find_all('a', href=True):
            urls[a['href']] = a.getText()
        return urls
    except Exception as e:
        print(e)
        return None


def retrieve_filename_and_extension(response=None, url=None):
    file_extensions = ["pdf", "docx", "doc", "xls", "xlsx", "csv"]
    content_type_list = ["pdf", "word", "excel", "officedocument"]
    try:
        filename = url.split("/")[-1]
        for e in file_extensions:
            if e in filename:
                return filename

        content_type = response.headers['content-type']
        for c in content_type_list:
            if c in content_type.lower():
                extension = mimetypes.guess_extension(content_type)
                if not extension:
                    filename = url.split("/")[-2] + ".{}".format(c)
                else:
                    filename = url.split("/")[-1] + extension
                return filename

        return filename + ".pdf"
    except Exception:
        filename = url.split("/")[-1]
        return filename + ".pdf"


def download_file(url=None, outfolder=None, use_header=False):
    """
    Download and save file if possible
    :param url:
    :param outfolder:
    :return:
    """
    try:
        if use_header:
            session = requests.session()
            file = session.get(url, headers=HEADER, allow_redirects=True, verify=False)
        else:
            file = requests.get(url, allow_redirects=True, verify=False)

        if file.status_code != 200:
            return None
        filename = retrieve_filename_and_extension(response=file, url=url)
        fpath = os.path.join(outfolder, filename)
        open(fpath, 'wb').write(file.content)
        return filename
    except Exception:
        return None


def create_full_url_from_part(base_url=None, content_url=None):
    """
    returns a full URL from base and part
    :param base_url:
    :param part_url:
    :return:
    """
    if len(content_url) < 6:
        return None

    parsed = list(urlparse(content_url))
    if parsed[0] == "http" or parsed[0] == "https":
        return content_url

    # first attempt
    full = urljoin(base_url, content_url)
    response = func_timeout(20, load_url, args=(full,))
    if response.status_code == 200:
        return full

    parsed_uri = urlparse(base_url)
    root_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    root_url2 = root_url[:-1]
    if root_url2[:-1] != "/":
        if content_url[0] == "/":
            revised_link = root_url + content_url[1:]
        else:
            revised_link = root_url + content_url
    else:
        revised_link = root_url2 + content_url

    response = func_timeout(20, load_url, args=(revised_link,))
    if response.status_code == 200:
        return revised_link

    return None


def download_content(content_url=None, output_folder=None, user_agent=False):
    """
    Given a list of links extracted from seed list of websites
    download relevant content
    :param urls: a dict object with country details and urls
    :param key_words: key words to determine what content to download
    :return:
    """
    try:
        if has_downloadable_content(content_url):
            if user_agent:
                return download_file(url=content_url, outfolder=output_folder, use_header=True)
            return download_file(url=content_url, outfolder=output_folder)
    except Exception:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))


def translate_key_words(dest_lan=None, en_key_words=None):

    out_key_words = []
    for v in en_key_words:
        try:
            # Try Goslate
            gs = goslate.Goslate()
            en_text = gs.translate(v, dest_lan)
            out_key_words.append(en_text)
        except Exception as e:
            pass
        try:
            # Googletrans
            translator = Translator()
            en_text = translator.translate(v, dest=dest_lan).text
            out_key_words.append(en_text)
        except Exception as e:
            pass
        try:
            # Translate
            translator = translate.Translator(to_lang=dest_lan, from_lang="en")
            translation = translator.translate(v)
            out_key_words.append(translation)
        except Exception as e:
            continue

    return out_key_words


def get_relevant_links(links=None, key_words=None, src_lan=None,
                       include_any_downloadable=False, translate_page=False):

    if translate_page:
        key_words = translate_key_words(dest_lan=src_lan, en_key_words=key_words)

    relevant_urls = []
    for k, v in links.items():
        if include_any_downloadable:
            for d in ["download", "pdf", "docx", "doc", "xls", "xlsx", "csv", "upload", "file"]:
                if d in k.lower() or d in v.lower():
                    relevant_urls.append(k)
                    break

    for k in relevant_urls:
        del links[k]

    for k, v in links.items():
        for w in key_words:
            if w in k.lower() or w in v.lower():
                relevant_urls.append(k)
                break
    return relevant_urls

