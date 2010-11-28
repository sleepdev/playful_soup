
import BeautifulSoup
import json
import re
import time
import tornado.escape
import tornado.httpclient
import types
import urlparse

utf8 = tornado.escape.utf8
Soup = BeautifulSoup.BeautifulSoup

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
        if '://' not in url:
            _url = urlparse.urljoin( context['base_url'], url )  
        else:
            _url = url

        parsed_url = urlparse.urlparse(_url)
        context['url'] = _url
        context['base_url'] = parsed_url.scheme + '://' + parsed_url.netloc
        try:
            time.sleep(1)
            doc = tornado.httpclient.HTTPClient().fetch(_url).body
            return commands[0]( context, doc, commands[1:] )
        except tornado.httpclient.HTTPError, x:
            print x
    return f



@typecheck( str, str )
def _select( document, selector ):
    "jQuery style selectors"

    if selector=='':
        return [document]

    if selector=='[innerHTML]':
        try:
            return [''.join( str(c) for c in Soup(document).contents[0].contents )]
        except Exception, x:
            print x
            return []       

    if ' ' in selector:
        this,next = selector.split(' ',1)
    else:
        this,next = selector,''


    if re.match('^![0-9]+$',this):
        try:
            return _select( str(Soup(document).contents[0].contents[int(this[1:])]) ,next)
        except IndexError,x:
            print x
            return []
    elif re.match("^\\[[a-zA-Z]+\\]$",this):
        return [str( Soup(document).contents[0][this[1:-1]] )]
    else:
        tag     = re.compile('[a-zA-Z]+')
        hasattr = re.compile('\\[(?P<attr>[a-zA-Z]+)\\]')
        rgxattr = re.compile('\\[(?P<attr>[a-zA-Z]+)(?P<op>[^\\"]+)(?P<pat>[^\\]]+)\\]')
        id      = re.compile('#(?P<id>[_a-zA-Z0-9]+)')
        cls     = re.compile('\\.(?P<class>[_a-zA-Z0-9]+)')
        ind     = re.compile('![0-9]+')

        m = tag.match(this)
        if m:
            name = this[ :m.end() ]
            this = this[ m.end(): ]
        else:
            if this.startswith('*'):
                this = this[1:]
            name = True
        
        kwargs = {}
        while this!="":
            if hasattr.match(this):
                m = hasattr.match(this)   
                kwargs[m.group('attr')] = True
            elif id.match(this):      
                m = id.match(this)
                kwargs["id"] = m.group('id')
            elif cls.match(this):
                m = cls.match(this)
                kwargs["class"] = cls.match(this).group('class')
            elif rgxattr.match(this):
                m    = rgxattr.match(this)
                attr = m.group('attr')
                op   = m.group('op')
                pat  = str(json.loads(m.group('pat')))
 
                if not isinstance(pat,str): 
                    raise Exception("Invalid Selector: %s" % this)
                if op=='=':    kwargs[attr] = (lambda a: a and a==pat)
                elif op=='~=': kwargs[attr] = (lambda a: a and pat in a.split(' '))
                elif op=='^=': kwargs[attr] = (lambda a: a and a.startswith(pat) )
                elif op=='$=': kwargs[attr] = (lambda a: a and a.endswith(pat) )
                elif op=='|=': kwargs[attr] = (lambda a: a and pat in a.split('-'))
                else: raise Exception("Invalid Selector: %s" % this)
            else:   
                raise Exception("Invalid Selector: %s" % this)
            this = this[ m.end(): ]
         
        return reduce(lambda a,b: a+b, (_select(str(o),next) for o in Soup(document).findAll( name, kwargs )), [] )


            

@typecheck( str )
def select( selector ):
    "jQuery style selector"
    @typecheck(dict,str,list)
    def f( context, document, commands ):
        for doc in _select( document, selector ):
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
        for url in _select( document, selector ):
           jumpto(url)(context,document,commands)
    return f




@typecheck(types.FunctionType,str)
def cleanse( post, format ):
    "format and validate data before submitting to a data sink"
    def f( context, document, commands ):
        assert len(commands)==0
        print context
    return f



@typecheck(list)
def crawl( site ):
    site[0]( {}, "", site[1:] )

















