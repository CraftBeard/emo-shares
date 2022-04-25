from spiders import xueqiu as xq
import properties as pr
import logging

logging.getLogger().setLevel(logging.INFO)

xq_stock = xq.stock(pr.STOCKS[0]['code'])
xq_stock.crawl_data()