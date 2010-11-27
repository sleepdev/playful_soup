
import BeautifulSoup
import httpclient


def jumpto( url ):
    "start spidering website"
    def f( context, document, commands ):
        pass
    return f


def select( selector="" ):
    "jQuery style selector"
    def f( context, document, commands ):
        pass #TODO
    return f



def extract( selectors=[] ):
    "extract data from document and add to context"
    def f( context, document, commands ):
        new_context = {}
        new_context.update( context )
        for k in data_selectors:
            if isinstance(data_selectors[k],k):
                new_context[k] = select(document,data_selectors[k])
            else
                for v in data_selectors[k]:
                    new_context[k] = select(document,v)
        return [(new_context,document)]
    return f



def follow( selector="" ):
    "follow link to new document"
    def f( context, document, commands ):
        return [
            ( ctx, httpclient.HTTPClient().fetch(url).body ) 
            for (ctx,url) in select(selector)( context, document )
        ]
    return f



def cleanse( post, format ):
    "format and validate data before submitting to a data sink"
    def f( context, document, commands ):
        assert len(commands)==0
        pass
    return f


















