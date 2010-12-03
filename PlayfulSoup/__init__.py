
import BeautifulSoup
import json
import logging
import re
import time
import tornado.escape
import tornado.httpclient
import types
import urlparse

"""
Commands
  jumpto   - start a crawler at the given urls (FIRST) 
  follow   - select and follow links from body text
  extract  - select and save data from page text
  commit   - validate and send data (LAST)

All commands take 1 application to configure and another to execute.
The second application is given 3 arguments.
  env : dict = the current environment of the crawler
  body: str  = the current page being examined
  cont: list = the list of commands to be applied to the results of this function
"""



utf8 = tornado.escape.utf8
Soup = BeautifulSoup.BeautifulSoup



def jumpto( *urls ):
    "start spidering a website"    
    def f( context, document, commands ):
        for url in urls:
            ctx = {}
            ctx.update( context ) 
            parsed_url = urlparse.urlparse(url)
            ctx['url'] = [url]
            ctx['base_url'] = [parsed_url.scheme + '://' + parsed_url.netloc]
            try:
                time.sleep(1)
                doc = tornado.httpclient.HTTPClient().fetch(url).body
                commands[0]( ctx, doc, commands[1:] )
            except tornado.httpclient.HTTPError, x:
                logging.error( str(x) )
    return f

#DONE
def flatten( ls ):
  return reduce( lambda a,b: a+b, ls, [] )

def do_select( document, selectors ):
    opt = [document]
    for sel in selectors.split():
        opt = flatten([ _select(doc,sel) for doc in opt ])
    return opt
 
def _select( document, selector ):
    "jQuery style selectors"

    if selector=='[innerHTML]':
        try:
            return [''.join( str(c) for c in Soup(document).contents[0].contents )]
        except Exception, x:
            logging.error( str(x) )
            return []       

    elif re.match("^\\[[a-zA-Z]+\\]$",this):
        return [str( Soup(document).contents[0][this[1:-1]] )]

    elif re.match('^![0-9]+$',selector):
        try:
            return [ str(Soup(document).contents[0].contents[int(selector[1:])]) ]
        except IndexError,x:
            print x
            return []

    else:
    
        ops = [
            [re.compile('(?P<name>[a-zA-Z0-9]+)'), (lambda g: {'$name$': g['name']}) ],
            [re.compile('\\[(?P<attr>[a-zA-Z]+)\\]'), (lambda g: {g['attr']: True}) ],
            [re.compile('\\[(?P<attr>[a-zA-Z]+)(?P<op>[^\\"]+)(?P<pat>[^\\]]+)\\]') ],
            (lambda g: 
                { g['attr']: (lambda a: a and a==pat) }                    if g['op']=='=' else
                { g['attr']: (lambda a: a and pat in a.split(' ')) }       if g['op']=='~=' else
                { g['attr']: (lambda a: a and a.startswith(pat)) }         if g['op']=='^=' else
                { g['attr']: (lambda a: a and a.endswith(pat)) }           if g['op']=='$=' else
                { g['attr']: (lambda a: a and pat in a.split('-')) }       if g['op']=='|=' else
                {}
            )],
            [re.compile('#(?P<id>[_a-zA-Z0-9]+)'), (lambda g: {'id': g['id']}) ],
            [re.compile('\\.(?P<class>[_a-zA-Z0-9]+)'), (lambda g: {'class': g['class']}) ],
        ]

        kwargs = {}
        while selector!='':
          matched = [rgx.match(selector),app for rgx,app in ops if rgx.match(selector)]
          assert len(matched)==1, ("Invalid Selector: %s" % selector)
          m,app = matched[0]
          kwargs.update( app(m.groups()) )
          selector = selector[ m.end(): ] 
        
        if "$name$" in kwargs:
            name = kwargs['$name$']
            del kwargs['$name$']
        else:
            name = True

        results = Soup(document).findAll( name, kwargs )         
        return [str(o) for o in Soup(document).findAll( name, kwargs ) ]



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
                for v in selectors[k]:
                    if v == None: continue
                    found = _select(document,v)    
                    if len(found)>= 1:
                        new_context[k] = found
                        break

            if next_context[k].startswith('/'):
                url = next_context[k]
                if '://' not in url:
                    assert 'base_url' in context
                    next_context[k] = urlparse.urljoin( context['base_url'][0], url ) 

        commands[0]( new_context, document, commands[1:] )
    return f



@typecheck(str)
def follow( selector ):
    "follow link to new document"
    @typecheck(dict,str,list)
    def f( context, document, commands ):
        for url in _select( document, selector ):
           jumpto(url)(context,document,commands)
    return f




@typecheck(types.FunctionType,str)
def commit( post, format ):
    "format and validate data before submitting to a data sink"
    @typecheck(dict,str,list)
    def f( context, document, commands ):
        assert len(commands)==0
        try:
            kwargs = {}
            attrs = format.split()
            for a in attrs:
                if a.endswith('[]'):
                    a = a[:-2]
                    assert isinstance(context[a],list)
                    assert all( isinstance(x,str) for x in context[a] )
                    kwargs[a] = context[a]
                else:
                    if isinstance(context[a],list):
                        assert len(context[a]) >= 1
                        assert all( isinstance(x,str) for x in context[a] )
                        kwargs[a] = context[a][0]
                    else:
                        assert isinstance(context[a],str)
                        kwargs[a] = context[a]
            post( **kwargs )
        except:
            print "Invalid Commit: ", context['url']
    return f


@typecheck(list)
def crawl( site ):
  site[0]( {}, "", site[1:] )
















