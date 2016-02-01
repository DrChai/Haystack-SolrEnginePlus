from haystack.constants import ITERATOR_LOAD_PER_QUERY
from haystack.query import SearchQuerySet
from django.utils import six

__author__ = 'Carrycat'


class CursorSearchQuerySet(SearchQuerySet):
    def __init__(self, using=None, query=None):
        self.is_using_cursor = False
        self.is_cursor_cached = True
        self.cursor_updated = False
        super(CursorSearchQuerySet, self).__init__(using=using, query=query)

    def __getitem__(self, k):
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice, six.integer_types)):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                    and (k.stop is None or k.stop >= 0))), \
            "Negative indexing is not supported."

        # Remember if it's a slice or not. We're going to treat everything as
        # a slice to simply the logic and will `.pop()` at the end as needed.
        if isinstance(k, slice):
            is_slice = True
            start = k.start

            if k.stop is not None:
                bound = int(k.stop)
            else:
                bound = None
        else:
            is_slice = False
            start = k
            bound = k + 1

        # We need check to see if we need to populate more of the cache.
        if len(self._result_cache) <= 0 or (None in self._result_cache[start:bound] and not self._cache_is_full()):
            try:
                if not self.query._next_cursor:
                    self._fill_cache(start, bound)
                else:
                    current_cache_max = self._result_cache.index(None)
                    bound = current_cache_max
                    if start > bound:
                        raise ValueError
            except StopIteration:
                # There's nothing left, even though the bound is higher.
                pass

        # Cache should be full enough for our needs.
        if is_slice:
            return self._result_cache[start:bound]
        else:
            return self._result_cache[start]

    def set_next_cursor(self, next_cursor, rows=10, cached=False):
        if cached:
            this = self
        else:
            this = self._clone()
        this.cursor_updated = True
        if next_cursor != this.query._next_cursor:  # diff from last check point
            this.is_cursor_cached = False
        elif next_cursor == this.query._next_cursor:
            this.is_cursor_cached = True
        this.query.add_next_cursor(next_cursor, int(rows))
        return this

    def get_next_cursor(self):
        if not self.query._next_cursor:
            return None  # undefined or return ERROR
        current_cache_max = 0
        if len(self._result_cache) > 0:
            try:
                current_cache_max = self._result_cache.index(None)
            except ValueError:
                current_cache_max = len(self._result_cache)
        if self.query._current_cursor is None or (not self.is_cursor_cached and self.cursor_updated):
            # if its first time called or
            # we cannot append with previous cached data and new updated data not been
            # queried we call _fill_cache(fill starts last iter)
            self._fill_cache(0, None)  # refill cache result
        elif self.is_cursor_cached and self.cursor_updated:
            # if we can append with previous cached data and new updated data not been
            # queried we call _fill_cache(fill starts last iter)
            self._fill_cache(current_cache_max, None)
        return self.query._next_cursor

    def _fill_cache(self, start, end, **kwargs):
        # Tell the query where to start from and how many we'd like.
        self.query._reset()
        if self.query._next_cursor is None:
            self.query.set_limits(start, end)

        results = self.query.get_results(**kwargs)
        self.cursor_updated = False
        if results is None or len(results) == 0:
            return False

        # Setup the full cache now that we know how many results there are.
        # We need the ``None``s as placeholders to know what parts of the
        # cache we have/haven't filled.
        # Using ``None`` like this takes up very little memory. In testing,
        # an array of 100,000 ``None``s consumed less than .5 Mb, which ought
        # to be an acceptable loss for consistent and more efficient caching.
        if len(self._result_cache) == 0 or self.is_cursor_cached is False:
            self._result_cache = [None for i in range(self.query.get_count())]

        if start is None:
            start = 0

        if end is None:
            end = self.query.get_count()

        to_cache = self.post_process_results(results)

        # Assign by slice.
        self._result_cache[start:start + len(to_cache)] = to_cache
        return True

    def _manual_iter(self):
        # If we're here, our cache isn't fully populated.
        # For efficiency, fill the cache as we go if we run out of results.
        # Also, this can't be part of the __iter__ method due to Python's rules
        # about generator functions.
        current_position = 0
        current_cache_max = 0

        while True:
            if len(self._result_cache) > 0:
                try:
                    current_cache_max = self._result_cache.index(None)
                except ValueError:
                    current_cache_max = len(self._result_cache)

            while current_position < current_cache_max:
                yield self._result_cache[current_position]
                current_position += 1

            if self._cache_is_full():
                raise StopIteration

            if not self.cursor_updated:
                # since we dont need _fill_cache() at the bottom
                raise StopIteration
            # We've run out of results and haven't hit our limit.
            # Fill more of the cache.
            if not self._fill_cache(current_position, current_position + ITERATOR_LOAD_PER_QUERY):
                raise StopIteration

    def query_facet(self, func=None, field=None, query=None):
        """Adds faceting to a query for the provided field with a custom query."""
        clone = self._clone()
        clone.query.add_query_facet(func, field, query)
        return clone

    def _clone(self, klass=None):
        clone = super(CursorSearchQuerySet, self)._clone(klass=klass)
        clone.is_using_cursor = self.is_using_cursor
        clone.is_cursor_cached = self.is_cursor_cached
        clone.cursor_updated = self.cursor_updated
        return clone