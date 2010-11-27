
import BeautifulSoup
import httpclient

def crawl( commands ):
    "crawl (list) -> None \n"
    "chain several commands together to form a workflow"
    pass

def load_page( url ):
    "load_page (string) -> dict \n"
    "retrieve text of page given by url"
    pass

def follow_links( document, selector="a :href", data_selectors={} ):
    "follow_links([string],[dict]) -> (dict) -> [dict] \n"
    "extract links from text, optionally filtered by a pattern, and yield the text of the retrieved pages"
    pass

def extract( data_selectors={} ):
    "extract (dict) -> (dict) \n"
    "extract data from page and add to context"

def cleanse():
    "format and validate data before submitting to a data sink"
    pass


