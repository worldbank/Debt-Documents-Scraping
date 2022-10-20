import os
import time
import utils as ut
import multiprocessing
from datetime import datetime


class Downloader:
    EN_KEY_WORDS = {"debt": 1, "loan": 1, "statistical": 2, "statistics": 2, "bulletin": 4, "monthly": 4,
                    "report": 4, "strategy": 3, "fiscal": 5, "budget": 3}
    RU = ["долг", "ссуда", "статистический", "статистика", "бюллетень", "ежемесячно",
          "доклад", "стратегия", "фискальный", "бюджет"]
    FR = ["dette", "prête", "statistiques", "bulletin", "mensuelle", "rapport", "stratégie", "statistique", "budget",
          "fiscale"]
    ESP = ["deuda", "préstamo", "presupuesto", "fiscal", "estadística", "boletín", "informe", "mensual",
"estrategia"]

    def __init__(self, country_urls=None, output_dir=None, max_run_time=1200, country_lan=None):
        """

        :param country_urls: dict object of country ID and link
        :param output_dir: where to save data
        """
        self.country_urls = country_urls
        self.output_dir = output_dir
        self.co_output_dirs = {k: os.path.join(self.output_dir, k) for k, v in country_urls.items()}
        self.max_run_time = max_run_time
        self.country_lan = country_lan

    def create_output_dirs(self):

        for k, v in self.co_output_dirs.items():
            if not os.path.exists(v):
                os.makedirs(v)

    def download_mwi(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("MWI"))

        # downloadable links
        downloadable = []
        for k, v in links.items():
            if "download" in v.lower():
                downloadable.append(k)

        # download files
        for l in downloadable:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("MWI"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['MWI'])

    def download_ben(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_relevant_links(starter_url=self.country_urls.get("BEN"), key_words=self.EN_KEY_WORDS)

        # download files
        for l in links:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("BEN"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['BEN'])

    def download_rwa(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_relevant_links(starter_url=self.country_urls.get("RWA"), key_words=self.EN_KEY_WORDS)

        # download files
        for l in links:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("RWA"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['RWA'])

    def download_gha(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_relevant_links(starter_url=self.country_urls.get("GHA"), key_words=self.EN_KEY_WORDS)

        # download files
        for l in links:
            ut.download_content(content_url=l, output_folder=self.co_output_dirs['GHA'])

    def download_ind(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        base_url = self.country_urls.get("IND")
        links_pg1 = ut.get_links_from_target_sites(url=base_url)
        links_pg2 = ut.get_links_from_target_sites(url=base_url + "?page=1")
        links_pg3 = ut.get_links_from_target_sites(url=base_url + "?page=2")
        links_pg4 = ut.get_links_from_target_sites(url=base_url + "?page=3")

        # download files
        for pg in [links_pg1, links_pg2, links_pg3, links_pg4]:
            downloads = []
            for k, v in pg.items():
                if "download" in v.lower():
                    downloads.append(k)
            for l in downloads:
                ut.download_content(content_url=l, output_folder=self.co_output_dirs['IND'])

    def download_gmb(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("GMB"))

        # downloadable links
        downloadable = []
        for k, v in links.items():
            if "download" in v.lower() or "download" in k.lower():
                downloadable.append(k)

        # download files
        for l in downloadable:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("GMB"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['GMB'])

    def download_eth(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("ETH"))

        # download files
        for l in links:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("ETH"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['ETH'])

    def download_bih(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("BIH"))

        # download files
        for l in links:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("BIH"), content_url=l)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['BIH'])

    def download_grd(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("GRD"))

        # downloadable links
        downloads = []
        for k, v in links.items():
            for w in self.EN_KEY_WORDS:
                if w in k.lower() or w in v.lower():
                    downloads.append(k)

        # download files
        for l in downloads:
            # full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("GRD"), content_url=l)
            ut.download_content(content_url=l, output_folder=self.co_output_dirs['GRD'])

    def download_arm(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # TODO: improve speed
        # get links
        links = {"/en/page/annual_reports/": 4, '/en/page/monthly_statistical_bulletin/': 1,
                 "en/page/summary_of_monthly_bulletin/": 2, '/en/page/summary_data_on_public_debt/': 1,
                 "en/page/treasury_direct1/": 2}

        for k, v in links.items():
            full = ut.create_full_url_from_part(base_url=self.country_urls.get("ARM"), content_url=k)
            if v == 1:
                links = ut.get_links_from_target_sites(full)
                for l in list(links.keys()):
                    ut.download_content(content_url=l, output_folder=self.co_output_dirs['ARM'])
            else:
                for i in range(1, v + 1):
                    pg = full + "{}".format(i)
                    pg_links = ut.get_links_from_target_sites(pg)
                    for k in list(pg_links.keys()):
                        ut.download_content(content_url=k, output_folder=self.co_output_dirs['ARM'])

    def download_khm(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # TODO: make this smarter, too much hardcoding
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("KHM"))

        # downloadable links
        downloads = []
        for k, v in links.items():
            for w in self.EN_KEY_WORDS:
                if w in k.lower() or w in v.lower():
                    if "en" in k.lower() and k[-4:] == "html":
                        downloads.append(k)

        # download files
        for l in downloads:
            links2 = ut.get_links_from_target_sites(url=l)
            for ll, v in links2.items():
                if "download" == v.lower():
                    ut.download_content(content_url=ll, output_folder=self.co_output_dirs['KHM'])

    def download_hnd(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # TODO: make this smarter, too much hardcoding
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("HND"))

        # downloadable links
        downloads = []
        for k, v in links.items():
            if "download" in k.lower() or "download" in v.lower():
                downloads.append(k)

        # download files
        for l in downloads:
            full_url = ut.create_full_url_from_part(base_url=self.country_urls['HND'], content_url=l)
            # new_url = ut.sanitize_url(full_url)
            ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['HND'])

    def download_ken(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # TODO: make this recursive and also faster
        # get links
        links = {"/41-external-public-debt-register.html": 1, "/44-others.html": 1,
                 "/42-medium-term-debt-strategy.html?limitstart=0": 1, "/157-annual-debt-management.html": 1,
                 "/42-medium-term-debt-strategy.html?limitstart=5": 1}

        for k, v in links.items():
            full = ut.create_full_url_from_part(base_url=self.country_urls.get("KEN"),
                                                content_url="/economy/category{}".format(k))
            links = ut.get_links_from_target_sites(full)
            for k, v in links.items():
                if "debt" in v.lower():
                    furl = ut.create_full_url_from_part(base_url=self.country_urls.get("KEN"), content_url=k)
                    ut.download_content(content_url=furl, output_folder=self.co_output_dirs['KEN'])

        # monthly bulletins
        monthly_url = ut.create_full_url_from_part(base_url=self.country_urls.get("KEN"),
                                                   content_url="/economy/category{}".format(
                                                       "/43-monthly-bulletins.html"))
        mlinks = ut.get_links_from_target_sites(url=monthly_url)
        for k, v in mlinks.items():
            if v.isdigit():
                furl = ut.create_full_url_from_part(base_url=self.country_urls.get("KEN"), content_url=k)
                links = ut.get_links_from_target_sites(url=furl)
                for l, d in links.items():
                    if "pdf" in d.lower():
                        full_path = ut.create_full_url_from_part(base_url=self.country_urls.get("KEN"), content_url=l)
                        ut.download_content(content_url=full_path, output_folder=self.co_output_dirs['KEN'])

    def download_kvo(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("KVO"))

        # download files
        for k, v in links.items():
            if "pdf" in k.lower():
                clean_content_url = k.strip()  # remove spaces
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("KVO"),
                                                        content_url=clean_content_url)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['KVO'])

    def download_lso(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("LSO"))

        # download files
        for k, v in links.items():
            if "pdf" in k.lower():
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("LSO"),
                                                        content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['LSO'])

    def download_mdg(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("MDG"))

        # download files
        for k, v in links.items():
            if "pdf" in k.lower():
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("MDG"),
                                                        content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['MDG'])

    def download_mdv(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        ut.download_content(content_url=self.country_urls.get("MDV"), output_folder=self.co_output_dirs['MDV'])

    def download_mda(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("MDA"), use_header=True)

        # download files
        for k, v in links.items():
            if "pdf" in k.lower():
                ut.download_content(content_url=k, output_folder=self.co_output_dirs['MDA'], user_agent=True)

    def download_nga(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("NGA"))

        # download files
        for k, v in links.items():
            if k.lower()[-4:] == "file" and "debt" in k.lower():
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("NGA"), content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['NGA'])

    def download_pak(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        ut.download_content(content_url=self.country_urls.get("PAK"), output_folder=self.co_output_dirs['PAK'])

    def download_png(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        # shamelessly hardcoding it here because the href links on the site
        # dont match the file URL
        # TODO: Fix the hardcoding below
        root = "https://www.treasury.gov.pg/html/public_debt/files/"
        files = ["2018/2018%20Quarter3%20Debt%20Issuance%20Plan.pdf",
                 "2017/2017%20Quarter%204%20IS%20Issuance%20Plan-Unsigned.pdf",
                 "2017/Quarter%203%20Issuance%20Plan%202017%20Announcement%20Final.pdf",
                 "2017/Quarter%202%20Issuance%20Plan%202017%20Announcement%20Final.pdf",
                 "2017/QUARTER%201%20ISSUANCE%20PLAN%202017%20FINAL.pdf",
                 "2017/2017%20ANNUAL%20DOMESTIC%20DEBT%20ISSUANCE%20PLAN.pdf",
                 "2016/Revised%20Quarter%204%20Issuance%20Plan%202016.pdf",
                 "2016/Quarter%203%20Issuance%20Plan%202016_Jul%2008.pdf",
                 "2016/2016%20Quarter%202%20Issuance%20Plan%20final.pdf",
                 "2016/2016%20Annual%20Domestic%20Debt%20Issuance%20Plan%2026%2002%202016_final.pdf"]

        # download files
        for f in files:
            ut.download_content(content_url=root + f, output_folder=self.co_output_dirs['PNG'])

    def download_wsm(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("WSM"))

        # download files
        for k, v in links.items():
            if k.lower()[-3:] == "pdf" or "download" in v.lower():
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("WSM"), content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['WSM'])

    def download_slb(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # TODO: remove the hardcoding
        months = ["January", "February", "March", "April", "May", "June",
                  "July", "August", "September", "October", "November", "December"]
        yrs = [i for i in range(2014, 2018)]

        base_url_monthly = "http://www.mof.gov.sb/Libraries/2016_Monthly_Debt_Reports/Debts_{}_{}.sflb.ashx?download=true"
        base_url_qt = "http://www.mof.gov.sb/Libraries/2016_Quarterly_Debt_Reports/{}_Quarter_{}.sflb.ashx"
        monthly_report = []
        for yr in yrs:
            for m in months:
                url = base_url_monthly.format(m, yr)
                qt_url = base_url_qt.format(m, yr)
                monthly_report.append(qt_url)
                monthly_report.append(url)

        for link in monthly_report:
            ut.download_content(content_url=link, output_folder=self.co_output_dirs["SLB"])

    def download_lka(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        ut.download_content(content_url=self.country_urls.get("LKA"), output_folder=self.co_output_dirs['LKA'])

    def download_tjk(self):
        """
        Function to download MALAWI files
        :param seed_rl:
        :return:
        """
        # TODO: need to use translator to review the documents
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("TJK"))

        # download files
        for k, v in links.items():
            if k.lower()[-3:] == "pdf":
                full_url = ut.create_full_url_from_part(base_url=self.country_urls.get("TJK"), content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs['TJK'])

    def download_uzb(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        ut.download_content(content_url=self.country_urls.get("UZB"), output_folder=self.co_output_dirs['UZB'])

    def download_vut(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        ut.download_content(content_url=self.country_urls.get("VUT"), output_folder=self.co_output_dirs['VUT'])

    def download_cog(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("CAF"))

        for k, v in links.items():
            if "pdf" in k.lower():
                ut.download_content(content_url=k, output_folder=self.co_output_dirs['CAF'])

    def download_cmr(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("CMR"))

        for k, v in links.items():
            if "pdf" in k.lower():
                ut.download_content(content_url=k, output_folder=self.co_output_dirs['CMR'])

    def download_cpv(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("CPV"))
        relevant_links = ut.get_relevant_links(key_words=self.EN_KEY_WORDS, src_lan="fr", links=links, translate=True)
        if not relevant_links:
            print("No relevant links on website")
            return

        for k, v in links.items():
            if "pdf" in k.lower():
                ut.download_content(content_url=k, output_folder=self.co_output_dirs['CMR'])

    def download_guy(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """

        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("GUY"))

        for k, v in links.items():
            try:
                more_links = []
                if v[-4:].isdigit():
                    more_links.append(k)
                if "download" in k.lower() or "download" in v.lower():
                    ut.download_content(content_url=k, output_folder=self.co_output_dirs['GUY'])
                for m in more_links:
                    tmp_links = ut.get_links_from_target_sites(url=m)
                    for key, val in tmp_links.items():
                        if "download" in key.lower() or "download" in val.lower():
                            ut.download_content(content_url=key, output_folder=self.co_output_dirs['GUY'])
            except Exception as e:
                continue

    def download_hti(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("HTI"))
        base_url = self.country_urls["HTI"]
        outdir = self.co_output_dirs['HTI']
        key_words = self.EN_KEY_WORDS
        p = multiprocessing.Process(target=ut.filter_and_download, name="Recursive Download",
                                    args=(links, True, key_words, "fr", base_url, outdir, 10))
        print("Starting at {}".format(time.asctime()))
        p.start()

        # ut.filter_and_download(url_links=links, download_everything=True, keywords=self.EN_KEY_WORDS,
        #                        page_lan="fr", country_base_url=self.country_urls["HTI"],
        #                        output_dir=self.co_output_dirs['HTI'], max_time=5)

        # Wait 10 seconds for foo
        time.sleep(self.max_run_time)

        # Terminate foo
        p.terminate()

        # Cleanup
        p.join()

    def download_lao(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("LAO"))
        visits = {k: 0 for k in list(links.keys())}
        base_url = self.country_urls["LAO"]
        outdir = self.co_output_dirs['LAO']
        key_words = self.EN_KEY_WORDS
        p = multiprocessing.Process(target=ut.filter_and_download, name="Recursive Download",
                                    args=(links, True, key_words, "en", base_url, outdir, visits))
        print("Starting at {}".format(time.asctime()))
        p.start()

        # Run for 10 minutes
        time.sleep(self.max_run_time)

        # Terminate foo
        p.terminate()

        # Cleanup
        p.join()

    def download_mmr(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # get links
        links = ut.get_links_from_target_sites(url=self.country_urls.get("MMR"))
        visits = {k: 0 for k in list(links.keys())}
        base_url = self.country_urls["MMR"]
        outdir = self.co_output_dirs['MMR']
        key_words = self.EN_KEY_WORDS
        p = multiprocessing.Process(target=ut.filter_and_download, name="Recursive Download",
                                    args=(links, True, key_words, "en", base_url, outdir, visits))
        print("Starting at {}".format(time.asctime()))
        p.start()

        # Run for 10 minutes
        time.sleep(self.max_run_time)

        # Terminate foo
        p.terminate()

        # Cleanup
        p.join()

    def download_som(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        debt_docs_url = 'http://www.mof.gov.so/publication?f%5B0%5D=publication_category_taxonomy_term_name%3AGovernment%20debt'
        links = ut.get_links_from_target_sites(url=debt_docs_url, use_header=True)

        # remove all query pages
        relevant_urls = {}
        for k, v in links.items():
            if "publication_category_taxonomy_term_name" in k:
                continue
            if "debt" in k.lower() and "debt" in v.lower():
                relevant_urls[k] = v

        visits = {k: 0 for k in list(links.keys())}
        base_url = self.country_urls["SOM"]
        outdir = self.co_output_dirs['SOM']
        key_words = self.EN_KEY_WORDS
        p = multiprocessing.Process(target=ut.filter_and_download, name="Recursive Download",
                                    args=(relevant_urls, True, key_words, "en", base_url, outdir, visits))
        print("Starting at {}".format(time.asctime()))
        p.start()

        # Run for 10 minutes
        time.sleep(self.max_run_time)

        # Terminate foo
        p.terminate()

        # Cleanup
        p.join()

    def download_ssd(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["SSD"], use_header=True)

        relevant_urls = ut.get_relevant_links(links=links, key_words=self.EN_KEY_WORDS, include_any_downloadable=True)

        for i in relevant_urls:
            ut.download_content(content_url=i, output_folder=self.co_output_dirs["SSD"])

    def download_tgo(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["TGO"])

        relevant_urls = ut.get_relevant_links(links=links, key_words=self.EN_KEY_WORDS, include_any_downloadable=True)

        for i in relevant_urls:
            if ut.has_downloadable_content(i):
                ut.download_content(content_url=i, output_folder=self.co_output_dirs["TGO"])

    def download_tmp(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["TMP"])

        relevant_urls = ut.get_relevant_links(links=links, key_words=self.EN_KEY_WORDS, include_any_downloadable=True)

        for i in relevant_urls:
            if ut.has_downloadable_content(i):
                ut.download_content(content_url=i, output_folder=self.co_output_dirs["TMP"])

    def download_tza(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["TZA"])
        for k, v in links.items():
            full_url = self.country_urls["TZA"] + k
            if ut.has_downloadable_content(full_url):
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs["TZA"])

    def download_zmb(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        base_url = "https://www.mof.gov.zm/download/public-debt-reports/"
        annual_reports = [base_url + "annual-public-debt-reports/"
                          + "debt_stock_{}.pdf".format(i) for i in range(2005, 2013)]

        medium_term = base_url + "medium-term-debt-strategy/Medium-Term-Debt-Strategy-2017-2019.pdf"
        quarterly = base_url + "quarterly-public-debt-reports/fy-2016/01._public_debt_report_end-may-16.pdf"
        all_links = annual_reports + [medium_term] + [quarterly]

        for l in all_links:
            if ut.has_downloadable_content(l):
                ut.download_content(content_url=l, output_folder=self.co_output_dirs["ZMB"])

    def download_zwe(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["ZWE"]
        outdir = self.co_output_dirs['ZWE']
        max_run_time = 120
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=True)

    def download_afg(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["AFG"], use_header=True)

        # remove all query pages
        relevant_urls = ut.get_relevant_links(links=links, key_words=self.EN_KEY_WORDS, include_any_downloadable=True)
        for link in relevant_urls:
            ut.download_content(content_url=link, output_folder=self.co_output_dirs["AFG"])

    def download_bfa(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["BFA"])

        for k, v in links.items():
            if "pdf" in k.lower():
                full_url = ut.create_full_url_from_part(base_url=self.country_urls["BFA"], content_url=k)
                ut.download_content(content_url=full_url, output_folder=self.co_output_dirs["BFA"])

    def download_bol(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        links = ut.get_links_from_target_sites(url=self.country_urls["BOL"])
        rl = ut.get_relevant_links(translate=True, key_words=self.EN_KEY_WORDS, links=links)

        for link in rl:
            ut.download_content(content_url=link, output_folder=self.co_output_dirs["BOL"])

    def download_btn(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["BTN"]
        outdir = self.co_output_dirs['BTN']
        max_run_time = 1200
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url)

    def download_aze(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["AZE"]
        outdir = self.co_output_dirs['AZE']
        max_run_time = 1200
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=False)

    def download_mli(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["MLI"]
        outdir = self.co_output_dirs['MLI']
        max_run_time = 120
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        p = multiprocessing.Process(target=sc.download, args=(base_url, True))
        p.start()
        time.sleep(120)
        p.join()
        p.terminate()
        # sc.download(starter_url=base_url, use_func_timer=True)

    def download_kgz(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["KGZ"]
        outdir = self.co_output_dirs['KGZ']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="ru")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = self.RU
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=True)

    def download_vct(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        # for now use a query for debt
        ut.download_file(url="http://www.gov.vc/images/pdf_documents/SVG_Report_MTDS_2018_2020.pdf",
                            outfolder=self.co_output_dirs["VCT"], use_header=True)

    def download_multiple_countries(self, co_list=None):
        max_run_time = 1800
        for c in co_list:
            print()
            print("========================================")
            print("Working on country {}".format(c))
            print("========================================")
            try:
                base_url = self.country_urls[c]
                outdir = self.co_output_dirs[c]
                start_time = datetime.now()
                sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                                max_run_time_requests=20, page_lan=self.country_lan[c])
                # set base properties
                sc.start_time = start_time
                sc.include_any_downloadable = True
                sc.en_key_words = list(self.EN_KEY_WORDS.keys())
                if self.country_lan[c] == 'ru':
                    sc.key_words = self.RU
                elif self.country_lan[c] == 'fr':
                    sc.key_words = self.FR
                elif self.country_lan[c] == 'spanish':
                    sc.key_words = self.ESP
                else:
                    sc.key_words = sc.en_key_words
                sc.translate = False
                sc.country_base_url = base_url
                # run the scraper
                sc.download(starter_url=base_url, use_func_timer=True)
            except Exception as e:
                print("Failed to download because of error below")
                print(e)
                continue

    def run_downloader_single_country(self, country_list=None):
        """
        Runs downloader
        """
        self.create_output_dirs()

        for k, v in self.country_urls.items():
            if k in country_list:
                print("Downloading data for country: {}".format(k))
                func_name = "download_{}".format(k.lower())
                func = getattr(self, func_name)
                func()

    def download_com(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["COM"]
        outdir = self.co_output_dirs['COM']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="fr")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = self.FR
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=False)

    def download_gin(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["GIN"]
        outdir = self.co_output_dirs['GIN']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="fr")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = self.FR
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=False)

    def download_lbr(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["LBR"]
        outdir = self.co_output_dirs['LBR']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = list(self.EN_KEY_WORDS.keys())
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=False)

    def download_ner(self):

        base_url = self.country_urls["NER"]
        outdir = self.co_output_dirs['NER']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="fr")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = self.FR
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=False)

    def download_sle(self):
        """
        Function to download MALAWI files
        :param seed_url:
        :return:
        """
        base_url = self.country_urls["SLE"]
        outdir = self.co_output_dirs['SLE']
        max_run_time = 2400
        sc = ut.Scraper(seed_url=base_url, output_dir=outdir, max_run_time_downloads=max_run_time,
                        max_run_time_requests=20, page_lan="en")
        # set base properties
        sc.en_key_words = list(self.EN_KEY_WORDS.keys())
        sc.key_words = sc.en_key_words
        sc.include_any_downloadable = True
        sc.country_base_url = base_url

        # run the scraper
        sc.download(starter_url=base_url, use_func_timer=True)

    def run_downloader_multiple_countries(self, country_list=None):
        """
        Runs downloader
        """
        self.create_output_dirs()

        self.download_multiple_countries(co_list=country_list)
