import asyncio
import datetime
import multiprocessing
from math import ceil
import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup



def init_processes(l):
    """глобальная переменная для всех процессов"""
    global results
    results = l


def get_url(arr: list) -> list:
    return [item.get('href') for item in arr]


def get_catalogs() -> list:
    """получаем полный каталог"""
    r = requests.get('https://yacht-parts.ru/catalog/')
    soup = BeautifulSoup(r.text, features='html.parser')
    categories = soup.find_all('li', {'class': ['sect']})

    return [category.findNext('a').get('href') for category in categories]


def get_inf(soup, category: str) -> None:
    try:
        name = soup.select_one('#pagetitle').text
    except:
        name = "Нет названия"

    try:
        id = soup.select_one('.article > .value').text
    except:
        id = "Нет артикуля"

    try:
        price = soup.select_one('.prices_block > div > div').text
        price = price.strip()
    except:
        price = "Нет цены"

    try:
        img = soup.select_one('.slides > .offers_img > a').get('href')
        #img_urls = ', '.join(map(str, img))
    except:
        img = "Нет картинок"

    try:
        brand = soup.select_one('.brand_picture > img').get('title')
    except:
        brand = "Нет бренда"

    try:
        description = soup.select_one('.preview_text').text
    except:
        description = "Нет описания"

    results.append({'name': name, 'id': id, 'price': price, 'description': description,
                    'brand': brand, 'category': category, 'img_urls': img})


async def get_products_info(session, products: list, category: str) -> None:
    """"проходимся по каждому товару страницы"""
    for product in products:
        url = f'https://yacht-parts.ru/{product}'

        async with session.get(url=url) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, features='html.parser')
            get_inf(soup, category)


async def get_page_data(session, catalog: str, pages_count: int) -> None:
    """проходимся по всех страницам категори"""
    for page in range(1, pages_count + 1):
        url = f'https://yacht-parts.ru/{catalog}'+f'/?PAGEN_1={page}'

        async with session.get(url=url) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, features='html.parser')
            products = get_url(soup.select('.image_block > .image_wrapper_block > .thumb'))
            category = soup.select_one('#pagetitle').text
            await get_products_info(session, products, category)


def save_excel(data: list, filename: str) -> None:
    """сохранение результата в excel файл"""
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='data', freeze_panes=(1,0))
    writer.sheets['data'].set_column(0, 1, width=70)
    writer.sheets['data'].set_column(1, 2, width=18)
    writer.sheets['data'].set_column(2, 3, width=8)
    writer.sheets['data'].set_column(3, 4, width=9)
    writer.sheets['data'].set_column(4, 5, width=4)
    writer.sheets['data'].set_column(5, 6, width=10)
    writer.sheets['data'].set_column(6, 7, width=5)
    writer.close()
    print(f'Все сохранено в {filename}.xlsx\n')


async def gather_date(urls):
    """создаём tasks со всеми страницами категорий"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for catalog in urls:
            url = f'https://yacht-parts.ru/{catalog}'
            response = await session.get(url=url)
            soup = BeautifulSoup(await response.text(), features='html.parser')
            try:
                pages_count = int(soup.select('.nums > a')[-1].text)
            except:
                pages_count = 1
            tasks.append(asyncio.create_task(get_page_data(session, catalog, pages_count)))
        await asyncio.gather(*tasks)


async def scrape(urls):
    await gather_date(urls)


def scrape_wrapper(args):
    asyncio.run(scrape(args))


def multi_process(urls):
    """распределяем нагрузку на процессы"""
    manager = multiprocessing.Manager()
    results = manager.list()

    batches = []
    len_urls = len(urls)
    processes = ceil(len_urls/100)

    for i in range(0, len_urls, processes):
        batches.append(urls[i:i+processes])

    with multiprocessing.Pool(processes=processes, initializer=init_processes, initargs=(results,)) as p:
            p.map(scrape_wrapper, batches)

    save_excel(list(results), 'test')


if __name__ == "__main__":
    catalogs = get_catalogs()
    start = datetime.datetime.now()  # запишем время старта
    multi_process(catalogs)
    end = datetime.datetime.now()  # запишем время завершения кода
    total = end - start  # расчитаем время затраченное на выполнение кода
    print("Затраченное время:" + str(total))


