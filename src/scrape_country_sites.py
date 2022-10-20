import os
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat

import PyPDF2
import pandas as pd

import downloader as dl
import utils as ut

FILE_EXTENSIONS = ["pdf", "docx", "doc", "xls", "xlsx" "csv", "txt"]
EN_KEY_WORDS = ["debt", "loan", "fiscal", "budget"]
RU = ["долг", "ссуда", "статистический", "статистика", "бюллетень", "ежемесячно","доклад", "стратегия", "фискальный", "бюджет"]
FR = ["dette", "prête",  "mensuelle",  "budget","fiscale"]
ESP = ["deuda", "préstamo", "presupuesto", "fiscal", "informe"]
KEY_WORDS =EN_KEY_WORDS + FR + ESP + RU


def get_doc_title(file=None):
    try:
        open_file = open(file, mode='rb')
        if file[-3:] == "pdf":
            pdf_document = PyPDF2.PdfFileReader(open_file)
            doc_info = pdf_document.documentInfo
            title = doc_info["/Title"]

            title_relevance = "No"
            for w in KEY_WORDS:
                if w in title.lower():
                    title_relevance = "Yes"
                    break
            return {"docTitle": title, "titleRelevant": title_relevance}
        else:
            return {"docTitle": "TBD", "titleRelevant": "TBD"}
    except Exception:
        return {"docTitle": "TBD", "titleRelevant": "TBD"}


def clean_up_files_in_country_dir(co_dir=None):
    co_files = {}
    file_lst = os.listdir(co_dir)
    cnt_valid_files = 0
    file_list = []

    for file in file_lst:
        fpath = os.path.join(co_dir, file)
        file_ext = file[-3:]
        if file_ext not in FILE_EXTENSIONS:
            os.remove(fpath)
            continue
        cnt_valid_files += 1
        file_list.append(fpath)

    co_files["fileList"] = file_list
    co_files["validFilesCnt"] = cnt_valid_files

    co_code = co_dir[-3:]
    return {co_code: co_files}


def load_docs_from_folder(docs_folder=None):
    co_folders = []
    for f in os.listdir(docs_folder):
        fpath = os.path.join(docs_folder, f)
        if os.path.isdir(fpath):
            co_folders.append(fpath)

    pool = ProcessPoolExecutor(max_workers=8)  # The one change
    results = pool.map(clean_up_files_in_country_dir, co_folders)
    all_country_files = [result for result in results]

    return all_country_files


def tag_document(doc_path=None, file_count=None, co_code=None, ):
    """
    Assign attributes to document
    :param doc_path:
    :param co_name:
    :param co_code:
    :param co_url:
    :return:
    """
    doc_meta = get_doc_title(file=doc_path)

    parts = doc_path.split("/")
    filename = parts[-1][:-3]
    file_relevance = "No"
    for w in EN_KEY_WORDS:
        if w in filename.lower():
            file_relevance = "Yes"
            break

    doc_full_meta = {"countryCode": co_code, "docFileName": filename,
                     "fileNameRelevant": file_relevance, "validFileCount": file_count}
    doc_full_meta.update(doc_meta)

    return doc_full_meta


def tag_document_bulk_process(docs_list=None, cnt=None, co_code=None):
    pool = ProcessPoolExecutor(max_workers=12)  # The one change
    results = pool.map(tag_document, docs_list, repeat(cnt), repeat(co_code))
    tagged_files = [result for result in results]

    return tagged_files


def add_total_relevant_docs(row):

    if row["fileNameRelevant"] == "Yes":
        return 1
    if row["titleRelevant"] == "Yes":
        return 1

    return 0


def generate_status_summary(df_dl=None, df_urls=None):

    df_dl["relevant"] = df_dl.apply(lambda x: add_total_relevant_docs(x), axis=1)
    df_grp = df_dl.groupby(["countryCode"]).agg({"relevant": "sum", "validFileCount": "first"})
    df_dl2 = df_grp.reset_index()
    df_dl2.rename(columns={"validFileCount": "totalFileDownloads", "relevant": "relevantFileDownloads"},
                  inplace=True)

    df = df_urls.merge(right=df_dl2, on="countryCode", how="left")
    df[["totalFileDownloads", "relevantFileDownloads"]] = df[["totalFileDownloads", "relevantFileDownloads"]].fillna(value=0)
    return df


def generate_docs_metadata(docs_dir=None, out_downloads_meta=None,
                           out_status_summary=None, weblinks_file=None):
    # Get a list of all docs
    docs_lst = load_docs_from_folder(docs_folder=docs_dir)

    all_tagged_docs = []
    for obj in docs_lst:
        id = list(obj.keys())[0]
        vals = obj[id]
        docs = vals["fileList"]
        tagged_docs = tag_document_bulk_process(docs_list=docs, cnt=vals["validFilesCnt"], co_code=id)
        all_tagged_docs += tagged_docs

    df = pd.DataFrame(all_tagged_docs)
    df.to_csv(out_downloads_meta, index=False)

    # prepare su,,aru metadata
    df_sum = generate_status_summary(df_dl=df, df_urls=pd.read_csv(weblinks_file))
    df_sum.to_csv(out_status_summary, index=False)


def process_countries(country_web_links=None, downloads_dir=None,
                      country_list=None, multiple=False):
    """
    Gets the download results and saves into CSV
    :param results:
    :return:
    """
    urls = ut.get_weblinks(fpath=country_web_links)
    co_url = {k: v["targetUrl"] for k, v in urls.items()}
    co_lan = {k: v["lan"] for k, v in urls.items()}

    downloader = dl.Downloader(country_urls=co_url, output_dir=downloads_dir, max_run_time=120, country_lan=co_lan)
    if multiple:
        downloader.run_downloader_multiple_countries(country_list=list(co_url.keys()))
    else:
        downloader.run_downloader_single_country(country_list=['SLE'])


def main(scrape=False, metadata=False):
    base_dir = os.path.abspath("/Users/dmatekenya/Google-Drive/WBG/MTI/")
    downloads_folder = os.path.join(base_dir, "fileDownloadsComplete")
    weblinks_csv = os.path.join(base_dir, "docs/webLinks.csv")
    files_metadata = os.path.join(base_dir, "docs/filesMetaData.csv")
    status_metadata = os.path.join(base_dir, "docs/statusSummary.csv")

    if metadata:
        generate_docs_metadata(docs_dir=downloads_folder, out_downloads_meta=files_metadata,
                               weblinks_file=weblinks_csv, out_status_summary=status_metadata)

    if scrape:
        process_countries(country_web_links=weblinks_csv, downloads_dir=downloads_folder, multiple=False)

    print("DONE")


if __name__ == '__main__':
    main(scrape=False, metadata=True)
