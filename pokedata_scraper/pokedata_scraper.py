import os
import random
import json
import csv
import time
import pandas as pd
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from colorama import Fore

id_number = []
last_used_cookie = None
cookie_lock = asyncio.Lock()

async def load_cookies(cookie_file_path):
    global last_used_cookie, cookie_lock

    async with cookie_lock:
        all_cookies = [f for f in os.listdir(cookie_file_path) if f.endswith('.json')]
        if len(all_cookies) > 1:
            available_cookies = [cookie for cookie in all_cookies if cookie != last_used_cookie]
        else:
            available_cookies = all_cookies
        cookie_file = random.choice(available_cookies)
        last_used_cookie = cookie_file
    with open(os.path.join(cookie_file_path, cookie_file), 'r') as file:
        cookies = json.load(file)
    return cookies

async def set_cookies(context, cookies):
    for cookie in cookies:
        await context.add_cookies([cookie])

def read_csv_file():
    urls = []
    ids = []

    links_csv_path = r"./links/urls.csv"
    with open(links_csv_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            urls.append(row['url'])
            ids.append(row['id'])
    print(len(urls))
    return urls, ids

async def close_popup(page):
    xpath = "//button[.//*[local-name()='svg' and @data-testid='CloseIcon']]"
    loader_spinner_xpath = "//span[contains(@class, 'MuiCircularProgress-root')]"
    try:
        close_icon = await page.locator(xpath).element_handle(timeout=1000)
        if close_icon:
            await close_icon.click()
    except PlaywrightTimeoutError:
        pass
    try:
        await page.wait_for_selector(loader_spinner_xpath, timeout=1000)
        await page.wait_for_selector(loader_spinner_xpath, state='hidden', timeout=20000)
    except PlaywrightTimeoutError:
        pass

async def error_handling(index, page, retries=5):
    xpath = '//h2[contains(text(),"Application error: a client-side exception has occurred")]'
    loader_spinner_xpath = "//span[contains(@class, 'MuiCircularProgress-root')]"

    for _ in range(retries):
        try:
            await page.wait_for_selector(xpath, timeout=2000)
            error = await page.query_selector(xpath)
            if error:
                await page.reload()
                await page.wait_for_selector(loader_spinner_xpath, timeout=5000)
                await page.wait_for_selector(loader_spinner_xpath, state='hidden', timeout=20000)
        except PlaywrightTimeoutError:
            break

async def number_entries(index, page, url, context, cookie_file_path, retries=3):
    entries = []
    for i in range(retries):
        try:
            await page.wait_for_selector('div[data-field="sold_price"] > span', timeout=30000)
            elements = await page.query_selector_all('div[data-field="sold_price"] > span')
            entries = [await element.inner_text() for element in elements]

            if len(entries) == 50:
                print(Fore.WHITE + f'Rate Limit reached for URL {index}, refreshing...')
                cookies = await load_cookies(cookie_file_path)
                await context.clear_cookies()
                await set_cookies(context, cookies)
                await page.reload()
                await page.wait_for_load_state('networkidle')
                await error_handling(index, page, retries=3)
                await close_popup(page)
                await error_handling(index, page, retries=3)
                entries.clear()
                continue 
        except Exception as e:
            continue

    return entries

async def scrape_data(context, url, iD, index, cookie_file_path, retries=5):
    price, psa, bids, sold_date, item_id, title, main_url, id_number = [], [], [], [], [], [], [], []
    page = await context.new_page()
    
    try:
        await page.goto(url, timeout=150000)
        await page.wait_for_load_state('networkidle')
    except Exception as e:
        # print(Fore.RED + f"Error processing URL {index}: {e}")
        await page.close()
        return []

    print(Fore.YELLOW + f"Processing URL {index}: {url}")
    await error_handling(index, page, retries)
    await close_popup(page)
    await error_handling(index, page, retries)
    await number_entries(index, page, url, context, cookie_file_path, retries=5)
    
    while True:
        await asyncio.sleep(0.3)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        sold_prices = soup.select('div[data-field="sold_price"] > span')
        if not sold_prices:
            break
        else:
            for sold_price in sold_prices:
                price.append(sold_price.text.strip() if sold_price else '')
                main_url.append(url)
                id_number.append(iD)

        for psa_grade in soup.select('div[data-field="psa_grade"] > span'):
            psa.append(psa_grade.text.strip() if psa_grade else '')

        for num_bids in soup.select('div[data-field="num_bids"] > span'):
            bids.append(num_bids.text.strip() if num_bids else '')

        for date_sold in soup.select('div[data-field="date_sold"] > span'):
            sold_date.append(date_sold.text.strip() if date_sold else '')

        for ebay_item_id in soup.select('div[data-field="ebay_item_id"] > div > a > span'):
            item_id.append(int(ebay_item_id.text.strip()) if ebay_item_id else 0)

        for titles in soup.select('div[data-field="title"] > div > span'):
            title.append(titles.text.strip() if titles else '')

        next_page = page.locator('//button[@title="Go to next page"]//span')
        if await next_page.is_visible():
            await next_page.click()
            await page.wait_for_load_state('networkidle')
        else:
            break

    return list(zip(price, psa, bids, sold_date, item_id, title, id_number, main_url))

def save_to_csv(rows, filepath):
    file_exists = os.path.isfile(filepath)
    df = pd.DataFrame(rows, columns=['Sale Price (USD)', 'Grade', '#Bids', 'Date Sold', 'Listing Id', 'Title', 'ID Number', 'URL'])
    df['Listing Id'] = df['Listing Id'].apply(lambda x: f"'{x}")
    with open(filepath, 'a', newline='', encoding='utf-8-sig') as f:
        df.to_csv(f, index=False, header=not file_exists)

async def extract_data_from_url(url, id, index, cookie_file_path, output_csv_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-gpu',
                '--start-maximized',
                '--disable-dev-shm-usage',
                '--disable-logging',
                '--enable-automation',
                '--log-level=3',
                '--v=99',
                '--disable-popup-blocking',
            ]
        )
        context = await browser.new_context(
            java_script_enabled=True,
            bypass_csp=True,
            permissions=[],
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            device_scale_factor=1,
            has_touch=False,
            is_mobile=False,
            ignore_https_errors=True
        )
        
        await context.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "stylesheet", "media", "font", "texttrack", "object", "beacon", "csp_report", "imageset"] else route.continue_())
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ["en-US", "en"]});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            Object.defineProperty(navigator, 'vendor', {get: () => 'Google Inc.'});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """)
        cookies = await load_cookies(cookie_file_path)
        await context.clear_cookies()
        await set_cookies(context, cookies)
        
        data = await scrape_data(context, url, id, index, cookie_file_path)
        save_to_csv(data, output_csv_path)
        await browser.close()

async def main():
    urls, ids = read_csv_file()
    output_csv_path = r"./output/output.csv"
    cookie_file_path = r'./cookies'

    tasks = []
    for i in range(len(urls)):
        while len(tasks) >= 1:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            tasks = list(pending)
        
        task = asyncio.create_task(extract_data_from_url(urls[i], ids[i], i + 1, cookie_file_path, output_csv_path))
        tasks.append(task)
    await asyncio.gather(*tasks)
    print("The Scraper has successfully finished")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(Fore.GREEN + f"Total time to complete: {int(hours)}:{int(minutes)}:{seconds:.2f}")
