"""
searchsubmission.py
Handles form submissions and uses word discovery for spelling correction.
"""

from typing import Dict, Any
from .word_discovery_fulltest import WordDiscovery


def process_search_submission(query: str, session_id: str = '') -> Dict[str, Any]:
    """Process a search form submission."""
    result = {
        'success': True,
        'original_query': query,
        'corrected_query': '',
        'corrections': [],
        'tuple_array': [],
        'session_id': session_id,
        'error': None
    }

    if not query or not query.strip():
        result['success'] = False
        result['error'] = 'Empty query'
        return result

    try:
        discovery = WordDiscovery(verbose=False)
        output = discovery.process(query)

        result['corrected_query'] = output['corrected_query']
        result['corrections'] = output['corrections']
        result['terms'] = output['terms']
        result['stats'] = output['stats']
        result['ngrams'] = output['ngrams']

        _print_results(result)
        return result

    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
        return result


def process_search_submission_full(query: str, session_id: str = '') -> Dict[str, Any]:
    """Process search submission with full word discovery results."""
    result = {
        'success': True,
        'original_query': query,
        'session_id': session_id,
        'error': None
    }

    if not query or not query.strip():
        result['success'] = False
        result['error'] = 'Empty query'
        return result

    try:
        discovery = WordDiscovery(verbose=False)
        output = discovery.process(query)
        result.update(output)

        _print_full_results(result)
        return result

    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
        return result


def _print_results(result: Dict[str, Any]) -> None:
    print("=" * 50)
    print("SEARCH SUBMISSION PROCESSED:")
    print(f"  Original Query: {result['original_query']}")
    print(f"  Corrected Query: {result['corrected_query']}")
    print(f"  Session ID: {result['session_id']}")

    if result['corrections']:
        print("  Corrections Made:")
        for c in result['corrections']:
            print(f"    '{c.get('original', '')}' → '{c.get('corrected', '')}' (distance: {c.get('distance', 0)})")

    print("=" * 50)


def _print_full_results(result: Dict[str, Any]) -> None:
    print("=" * 50)
    print("SEARCH SUBMISSION PROCESSED (FULL):")
    print(f"  Original Query: {result.get('original_query', '')}")
    print(f"  Corrected Query: {result.get('corrected_query', '')}")
    print(f"  Stats: {result.get('stats', {})}")
    print(f"  Session ID: {result.get('session_id', '')}")

    if result.get('corrections'):
        print("  Corrections Made:")
        for c in result['corrections']:
            print(f"    '{c.get('original', '')}' → '{c.get('corrected', '')}'")

    print("=" * 50)