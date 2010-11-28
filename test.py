
from __init__ import jumpto, select, extract, follow, cleanse, crawl

def save_data( **kwargs ):
    print kwargs

data_sources = [

    #site: thehundreds.com
    [   
        jumpto  ( "http://shop.thehundreds.com/" ),

        select  ( 'a[href^="c-"]' ),
        extract ( {"tags":"[innerHTML]"} ),
        follow  ( "[href]" ),

        select  ( 'a[href^="p-"]' ),
        extract ( {"url":"[href]"} ),
        follow  ( "[href]" ),

        extract ( { 
            "title": "td.prodinfo [innerHTML]",
            "image_url": "div#item_color img [src]",
            "price": [ 'span.SalePrice [innerHTML] ~"[0-9]+\\.[0-9]{2}"', 
                        'span.RegularPrice [innerHTML] ~"[0-9]+\\.[0-9]{2}"' ],
            "description": "div#desc p.MsoNormal !0",
        } ),
        cleanse ( save_data, "url image_url[] title price description tags[]" )
    ],

]

if __name__=="__main__":
    for site in data_sources:
        crawl(site)
