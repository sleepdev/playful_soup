
import BeautifulSoup
import re
import tornado.httpclient
import types



def typecheck( *types ):
    def check( f ):
        def inner( *args ):
            try:
                assert len(types)==len(args)
                assert all( isinstance(t,T) for t,T in zip(args,types) )
                return f( *args )
            except AssertionError:
                raise TypeError("Expected %s but got %s" % 
                    ( types, [t.__class__ for t in args] ) 
                )
        return inner
    return check



@typecheck( str )
def jumpto( url ):
    "start spidering a website"
    
    @typecheck( dict, str, list )
    def f( context, document, commands ):
        doc = tornado.httpclient.HTTPClient().fetch(url).body
        return commands[0]( context, doc, commands[1:] )
    return f



@typecheck( str, str )
def _select( document, selector ):
    "jQuery style selectors"
    #print "_select: %s %s" % (document, selector)

    if selector=='':
        return document

    if ' ' in selector:
        this,next = selector.split(' ',1)
    else:
        this,next = selector,''

    if selector=="[innerHTML]":
        print "[innerHTML]", document
        return [''.join( str(c) for c in BeautifulSoup.BeautifulSoup(document).contents[0].contents )]
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
                attr_match = this[ 1:m.end()-1 ]
                assert tag.match(attr_match), 'Complicated attribute selectors not implemented %s' % attr_match
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
        
        print "select",name,kwargs 
        return reduce(lambda a,b: a+b, (_select(str(o),next) for o in BeautifulSoup.BeautifulSoup(document).findAll( name, kwargs )) )
            

@typecheck( str )
def select( selector ):
    "jQuery style selector"
    @typecheck(dict,str,list)
    def f( context, document, commands ):
        for doc in _select( document, selector ):
            print doc
            commands[0](context,doc,commands[1:])
    return f



@typecheck(dict)
def extract( selectors ):
    "extract data from document and add to context"
    @typecheck(dict,str,list)
    def f( context, document, commands ):
        new_context = {}
        new_context.update( context )
        for k in selectors:
            if isinstance(selectors[k],str):
                new_context[k] = _select(document,selectors[k])
            else:
                found = [ _select(document,v) for v in selectors[k] ]
                found = [ v for v in found if v!=None ]
                new_context[k] = found[0]
        return commands[0]( new_context, document, commands[1:] )
    return f


@typecheck(str)
def follow( selector ):
    "follow link to new document"
    def f( context, document, commands ):
        for url in _select( selector, document ):
            try:
                doc = tornado.httpclient.HTTPClient().fetch(url).body
                commands[0](ctx,doc,commands[1:])
            except httpclient.HTTPError:
                pass
    return f



@typecheck(types.FunctionType,str)
def cleanse( post, format ):
    "format and validate data before submitting to a data sink"
    def f( context, document, commands ):
        assert len(commands)==0
        print context, document
    return f



def crawl( site ):
    site[0]( {}, "", site[1:] )

















