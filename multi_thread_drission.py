import sys
import time
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from DrissionPage import Chromium, ChromiumOptions
from concurrent.futures import ThreadPoolExecutor, as_completed

#TODO:: Pagesave conection path
try:
    PAGESAVE_PATH = Path("D:/Sharma Danesh/Pagesave/Google_map_scrape/product_pagesave/12_11_2025")
    PAGESAVE_PATH.mkdir(parents=True, exist_ok=True)
except Exception as e:
    print(e)

df = pd.read_excel("inputs_location.xlsx")

st_tm1 = time.time()

co = ChromiumOptions()
co.set_argument('--no-sandbox')


def fetch_page(tab, urls):
    tab.listen.start("https://www.google.com/search")
    for row in urls:
        try:
            search_box = tab.ele('xpath://input[@id="searchboxinput"]')
            if search_box:
                fetch_id = row["fetch_ID"]
                fetch_Locations = row["Locations"]
                search_box.clear()
                search_box.click()
                search_box.input(fetch_Locations)
                time.sleep(1)

                tab.ele('xpath://button[@id="searchbox-searchbutton"]').click()
                time.sleep(5)  # Wait for search results to load
                scroll_container = tab.run_js('return document.querySelector(\'[aria-label^="Results for"]\')')

                if scroll_container:
                    scroll_pause_time = 3  # Increased pause to allow lazy load
                    prev_height = 0
                    same_height_count = 0
                    scroll_count = 0
                    page = 1
                    while True:
                        try:
                            res = tab.listen.wait(timeout=2)
                            parsed_url = urlparse(res.url)
                            params = parse_qs(parsed_url.query)
                            ech = params.get("ech")
                            print("page:", ech[0])
                            data = res.response.body
                            file_name1 = f"pagesaveID_{fetch_id}_pageno_{page}.html"
                            full_path1 = PAGESAVE_PATH / file_name1
                            page += 1
                            if res.response.status == 200:
                                with open(full_path1, 'w', encoding='utf-8') as f:
                                    f.write(data)
                                print(file_name1)
                        except:
                            pass
                        current_height = tab.run_js('return arguments[0].scrollHeight;', scroll_container)

                        if current_height == prev_height:
                            same_height_count += 1
                        else:
                            same_height_count = 0

                        if same_height_count >= 3:  # Waited 3 times and height didn't change
                            break

                        prev_height = current_height
                        tab.run_js('arguments[0].scrollTo(0, arguments[0].scrollHeight);', scroll_container)
                        scroll_count += 1
                        time.sleep(scroll_pause_time)

        except:pass

        time.sleep(1)
def create_and_load_tab(browser):
    tab = browser.new_tab()
    try:
        tab.get("https://www.google.com/maps")
        tab.wait.load_start()  # Ensure navigation starts
        print("Intialized tab with Google Maps")
    except Exception as e:
        print(f"Tab init warning: {e}")
    return tab


def fetch_all(browser, urls, num_tabs):
    url_batches = [urls[i::num_tabs] for i in range(num_tabs)]

    tabs = [browser.new_tab(url='https://www.google.com/maps') for _ in range(num_tabs)]

    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(num_tabs):
            executor.submit(fetch_page, tabs[i], url_batches[i])


if __name__ == '__main__':
    # port = 1978
    # tabs = 1
    # start_id = 0
    # end_id = 100000
    port = sys.argv[1]
    tabs = int(sys.argv[2])
    start_id = int(sys.argv[3])
    end_id = int(sys.argv[4])

    st_tm1 = time.time()

    urls = [{"fetch_ID":row.get("ID"), "Locations":row.get("Locations")} for _, row in df.iterrows()]
    browser = Chromium(f'127.0.0.1:{port}', co)
    av = urls[start_id:end_id]
    # print(av)
    fetch_all(browser, av, tabs)

    print(time.time() - st_tm1)