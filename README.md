# Haystack-SolrCursorPagination
Extending queryset and SolrBackend models for Django Haystack, that lets Django Haystack support Solr's Cursor Pagination 

## About
A light extension for [Haystack](https://github.com/django-haystack/django-haystack)'s Solr Backend. 

### CursorPagination
It supports Solr's cursorMark to request subsequent pages of sorted results.
Since current Haystack only provides Basic Pagination which could be ineffcient on fetching large number of results.

states on Apache Solr website:
>Performance Problems with "Deep Paging"
>
> In some situations, the results of a Solr search are not destined for a simple paginated user interface.  When you wish to fetch a very large number of sorted results from Solr to feed into an external system, using very large values for the start or rows parameters can be very inefficient.  Pagination using start and rows not only require Solr to compute (and sort) in memory all of the matching documents that should be fetched for the current page, but also all of the documents that would have appeared on previous pages. 

for more information about Cursor pagination, please go to [here](https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results/#PaginationofResults-FetchingALargeNumberofSortedResults:Cursors)

This extension still provides cached results as long as you are keeping fetch results in sequence.

### The Extended DisMax Query Parser (eDisMax) (in progressing)

## Usage
ToDo

## Installation
ToDo
