from haystack.backends import BaseEngine
from haystack.backends.solr_backend import SolrSearchBackend, SolrSearchQuery
from haystack.constants import VALID_FILTERS, FILTER_SEPARATOR, DEFAULT_ALIAS, ITERATOR_LOAD_PER_QUERY

__author__ = 'Carrycat'


class EventSolrSearchBackend(SolrSearchBackend):
    def _process_results(self, raw_results, highlight=False, result_class=None, distance_point=None):
        result = super(EventSolrSearchBackend, self)._process_results(raw_results,
                                                                      highlight=highlight,
                                                                      result_class=result_class,
                                                                      distance_point=distance_point)
        if hasattr(raw_results, 'nextCursorMark'):
            nextCursorMark = raw_results.nextCursorMark
            result['nextCursorMark'] = nextCursorMark
        return result

    def build_search_kwargs(self, query_string, sort_by=None, start_offset=0, end_offset=None,
                            fields='', highlight=False, facets=None,
                            date_facets=None, query_facets=None,
                            narrow_queries=None, spelling_query=None,
                            within=None, dwithin=None, distance_point=None,
                            models=None, limit_to_registered_models=None,
                            result_class=None, stats=None, cursorMark=None, cursor_rows=None):
        kwargs = super(EventSolrSearchBackend, self).build_search_kwargs(
            query_string, sort_by=sort_by, start_offset=start_offset, end_offset=end_offset,
            fields=fields, highlight=highlight, facets=facets,
            date_facets=date_facets, query_facets=None,
            narrow_queries=narrow_queries, spelling_query=spelling_query,
            within=within, dwithin=dwithin, distance_point=distance_point,
            models=models, limit_to_registered_models=limit_to_registered_models,
            result_class=result_class, stats=stats)
        if cursorMark is not None:
            kwargs['cursorMark'] = cursorMark
            kwargs.pop('rows', None)
            kwargs['rows'] = cursor_rows
            kwargs.pop('start')
        if kwargs.get('sort', None):
            if kwargs['sort'] == 'geodist() asc':
                kwargs['sort'] = 'geodist() asc,id asc'
            elif kwargs['sort'] == 'geodist() desc':
                kwargs['sort'] = 'geodist() desc,id desc'
        if query_facets is not None:
            kwargs['facet'] = 'on'
            query_list = []
            for func, field, value in query_facets:
                func = "{!%s}" % func if func else ""
                if field is None and value is None:
                        query_list.append("%s" % func)
                elif field and value:
                        query_list.append("%s%s:%s" % (func, field, value))
                else:
                    pass
            kwargs['facet.query'] = query_list
        return kwargs


class EventSolrSearchQuery(SolrSearchQuery):
    def __init__(self, using=DEFAULT_ALIAS):
        self._next_cursor = None
        self._current_cursor = None
        self.cursor_rows = None
        super(EventSolrSearchQuery, self).__init__(using=DEFAULT_ALIAS)

    def build_params(self, spelling_query=None, **kwargs):
        search_kwargs = super(EventSolrSearchQuery, self).build_params(spelling_query=spelling_query, **kwargs)
        if self._next_cursor:  # if next_cursor() is called
            # if self._next_cursor_cache:  # if cursor cache is set using cursor cache instead of next_cursor
            # search_kwargs['cursorMark'] = self._next_cursor_cache
            # else:
            search_kwargs['cursorMark'] = self._next_cursor
            search_kwargs['cursor_rows'] = self.cursor_rows
        return search_kwargs

    def run(self, spelling_query=None, **kwargs):
        """Builds and executes the query. Returns a list of search results."""
        final_query = self.build_query()
        search_kwargs = self.build_params(spelling_query, **kwargs)

        if kwargs:
            search_kwargs.update(kwargs)

        results = self.backend.search(final_query, **search_kwargs)
        self._results = results.get('results', [])
        self._hit_count = results.get('hits', 0)
        self._facet_counts = self.post_process_facets(results)
        self._stats = results.get('stats', {})
        if self._next_cursor:
            self._current_cursor = self._next_cursor
        self._next_cursor = results.get('nextCursorMark', None)  # update next cursor
        #
        # self._next_cursor_cache = results.get('nextCursorMark', None)  # X

        self._spelling_suggestion = results.get('spelling_suggestion', None)

    def add_next_cursor(self, next_cursor, rows):
        """
        set next cursorMark and rows
        """
        if not isinstance(next_cursor, str) or not isinstance(rows, int):
            raise AttributeError('The next_cursor must be a string')
        self.cursor_rows = rows
        self._next_cursor = next_cursor

    def add_query_facet(self, func, field, query):
        """Adds a query facet on a field."""
        from haystack import connections

        self.query_facets.append((func, connections[self._using].get_unified_index().get_facet_fieldname(field), query))

    def _clone(self, klass=None, using=None):
        clone = super(EventSolrSearchQuery, self)._clone(klass=klass, using=using)
        clone._next_cursor = self._next_cursor
        clone._current_cursor = self._current_cursor
        clone.cursor_rows = self.cursor_rows
        return clone


class SolrEngine(BaseEngine):
    backend = EventSolrSearchBackend
    query = EventSolrSearchQuery