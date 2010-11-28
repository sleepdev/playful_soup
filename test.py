
from __init__ import jumpto, select, extract, follow, cleanse, crawl
import sys


def save_data( **kwargs ):
    for k in kwargs:
        if kwargs[k]==None: 
            print '%s : None' % k
        elif len(kwargs[k])<200:
            print '%s : %s' % (k,kwargs[k])
        else:
            print '%s : %s' % (k,kwargs[k][:197]+'...')
    print '\n'

data_sources = [

    #site: thehundreds.com
    [   
        jumpto  ( "http://shop.thehundreds.com/" ),

        select  ( 'a[href^="c-"]' ),
        extract ( {"tags":"[innerHTML]"} ),
        follow  ( "[href]" ),

        select  ( 'a[href^="p-"]' ),
        follow  ( "[href]" ),

        extract ( { 
            "title": "td.prodinfo h2 [innerHTML]",
            "image_url": "div#item_color img [src]",
            "price": 'span.price [innerHTML]',
            "description": "div#desc p.MsoNormal !0",
        } ),
        cleanse ( save_data, "url image_url[] title price description tags[]" )
    ],

]

if __name__=="__main__":
    for site in data_sources:
        crawl(site)
