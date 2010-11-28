
import BeautifulSoup
import tornado.httpclient
import re


def jumpto( url ):
    "start spidering website"
    def f( context, document, commands ):
        doc = tornado.httpclient.HTTPClient().fetch(url).body
        return commands[0]( context, doc, commands[1:] )
    return f



def _select( selector, context, document ):
    if selector=='':
        return context, document

    if ' ' in selector:
        this,next = selector.split(' ',1)
    else:
        this,next = selector,''

    if selector=="[innerHTML]":
        pass #TODO
    else:
        tag     = re.compile('[a-zA-Z]+')
        attr    = re.compile('\\[[a-zA-Z]+([^\\"\\]]+(\\"[^\\"]*\\"))\\]')
        id      = re.compile('#[a-zA-Z]+')
        cls     = re.compile('\\.[a-zA-Z]+')

        m = tag.match(this)
        if m:
            name = this[ :m.end() ]
            this = this[ m.end(): ]
        else:
            name = True

        kwargs = {}
        while this!="":
            if attr.match(this):    
                m = attr.match(this)
                name = this[ :m.end() ]
                this = this[ m.end(): ]
            elif id.match(this):    
                m = id.match(this)
                kwargs["id"] = this[ 1:m.end() ]
                this = this[ m.end(): ]
            elif cls.match(this):   
                m = cls.match(this)
                kwargs["class"] = this[ 1:m.end() ]
            else:   
                raise Exception("Invalid Selector: %s" % this)

        return [ unicode(o) for o in BeautifulSoup.BeautifulSoup(document).findAll( name, kwargs ) ]
            


def select( selector="" ):
    "jQuery style selector"
    def f( context, document, commands ):
        for doc in _select(selector, context, document ):
            yield commands[0](context,doc,commands[1:])
    return f



def extract( selectors=[] ):
    "extract data from document and add to context"
    def f( context, document, commands ):
        new_context = {}
        new_context.update( context )
        for k in data_selectors:
            if isinstance(data_selectors[k],k):
                new_context[k] = select(document,data_selectors[k])
            else:
                for v in data_selectors[k]:
                    new_context[k] = select(document,v)
        return commands[0]( new_context, document, commands[1:] )
    return f



def follow( selector="" ):
    "follow link to new document"
    def f( context, document, commands ):
        for url in _select(selector, context, document ):
            try:
                doc = tornado.httpclient.HTTPClient().fetch(url).body
                yield commands[0](ctx,doc,commands[1:])
            except httpclient.HTTPError:
                pass
    return f



def cleanse( post, format ):
    "format and validate data before submitting to a data sink"
    def f( context, document, commands ):
        assert len(commands)==0
        pass
    return f


















