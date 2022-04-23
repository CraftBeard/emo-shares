from spiders import xueqiu as xq
import json

xq_stock = xq.stock(pr.STOCKS[0]['code'])
xq_res = xq_stock.get_post() 

