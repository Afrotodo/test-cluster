"""
word_discovery_edge_cases.py
============================
Django debug endpoint for testing WordDiscovery end-to-end.

URL:
    GET /debug/word-discovery/?q=your+query+here
    GET /debug/word-discovery/?category=keyboard_typos
    GET /debug/word-discovery/?run_all=1

SETUP:
    1. Add to urls.py:
       path('debug/word-discovery/', views.debug_word_discovery, name='debug_word_discovery'),

    2. Add to views.py:
       from .word_discovery_edge_cases import debug_word_discovery
"""

import time
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from .word_discovery_fulltest import (
    WordDiscovery,
    RAM_CACHE_AVAILABLE,
    vocab_cache,
    STOPWORDS,
    normalize_pos_string,
    get_fuzzy_suggestions_batch,
)


# =============================================================================
# EDGE CASE TEST DEFINITIONS
# =============================================================================

TEST_CASES = {

    # -------------------------------------------------------------------------
    # Simple single typos
    # -------------------------------------------------------------------------
    "simple_typos": {
        "description": "Basic single character errors",
        "cases": [
            {
                "query":  "the dog is bleu",
                "notes":  "Simple substitution: e/u swap → blue",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "she is hunrgy",
                "notes":  "Transposition: r and g swapped → hungry",
                "expect": {"hunrgy": "hungry"},
            },
            {
                "query":  "the quik brown fox",
                "notes":  "Missing letter c → quick",
                "expect": {"quik": "quick"},
            },
            {
                "query":  "she recieved a gift",
                "notes":  "Common misspelling ie vs ei → received",
                "expect": {"recieved": "received"},
            },
            {
                "query":  "the ballon is red",
                "notes":  "Missing repeated l → balloon",
                "expect": {"ballon": "balloon"},
            },
            {
                "query":  "she is sucessful",
                "notes":  "Missing repeated c and s → successful",
                "expect": {"sucessful": "successful"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Keyboard adjacent typos
    # -------------------------------------------------------------------------
    "keyboard_typos": {
        "description": "Errors from hitting adjacent keys on keyboard",
        "cases": [
            {
                "query":  "thr dog is brown",
                "notes":  "r is adjacent to e on keyboard → the",
                "expect": {"thr": "the"},
            },
            {
                "query":  "the cat ir cute",
                "notes":  "r is adjacent to s on keyboard → is",
                "expect": {"ir": "is"},
            },
            {
                "query":  "i luv music",
                "notes":  "u adjacent to i, missing o → love",
                "expect": {"luv": "love"},
            },
            {
                "query":  "thw quick brown fox",
                "notes":  "w is adjacent to e on keyboard → the",
                "expect": {"thw": "the"},
            },
            {
                "query":  "school iz fun",
                "notes":  "z is adjacent to s on keyboard → is",
                "expect": {"iz": "is"},
            },
            {
                "query":  "i wsnt to go home",
                "notes":  "s is adjacent to a on keyboard → want",
                "expect": {"wsnt": "want"},
            },
            {
                "query":  "the wotk is hard",
                "notes":  "o is adjacent to r on keyboard → work",
                "expect": {"wotk": "work"},
            },
            {
                "query":  "she playd the game",
                "notes":  "Missing e at end → played",
                "expect": {"playd": "played"},
            },
            {
                "query":  "i liek dogs",
                "notes":  "i and e transposed → like",
                "expect": {"liek": "like"},
            },
            {
                "query":  "the hoyse is big",
                "notes":  "o is adjacent to u on keyboard → house",
                "expect": {"hoyse": "house"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # POS disambiguation — context picks the right correction
    # -------------------------------------------------------------------------
    "pos_disambiguation": {
        "description": "POS pattern matching picks the right correction",
        "cases": [
            {
                "query":  "the sky is bleu today",
                "notes":  "[be] ___ [noun] → adjective blue not verb blew",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "the wind bleu hard",
                "notes":  "[noun] ___ [adverb] → verb blew not adjective blue",
                "expect": {"bleu": "blew"},
            },
            {
                "query":  "i red the book",
                "notes":  "[pronoun] ___ [article] → verb read not adjective red",
                "expect": {"red": "read"},
            },
            {
                "query":  "she through the ball",
                "notes":  "[pronoun] ___ [article] → verb threw",
                "expect": {"through": "threw"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Multiple errors — tests Redis batch pipeline
    # -------------------------------------------------------------------------
    "multiple_errors": {
        "description": "Multiple typos in one query — all sent to Redis in one batch",
        "cases": [
            {
                "query":  "teh dgo is bleu",
                "notes":  "Three errors — all sent to Redis in ONE pipeline batch",
                "expect": {"teh": "the", "dgo": "dog", "bleu": "blue"},
            },
            {
                "query":  "she iz vrey hunrgy",
                "notes":  "Three errors: keyboard, transposition, transposition",
                "expect": {"iz": "is", "vrey": "very", "hunrgy": "hungry"},
            },
            {
                "query":  "wher iz africn locatd",
                "notes":  "Four errors — tests multi-pass POS neighbor walk + batch",
                "expect": {"wher": "where", "iz": "is", "africn": "african", "locatd": "located"},
            },
            {
                "query":  "i cant wayt too see yuo",
                "notes":  "Multiple errors, some valid words stay unchanged",
                "expect": {"wayt": "wait", "yuo": "you"},
            },
            {
                "query":  "i am happppy today",
                "notes":  "Repeated letters — too many p's → happy",
                "expect": {"happppy": "happy"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Adjacent unknowns — hardest case, tests multi-pass neighbor walk
    # -------------------------------------------------------------------------
    "adjacent_unknowns": {
        "description": "Consecutive unknown words — tests multi-pass walk past unknowns",
        "cases": [
            {
                "query":  "i visited nw yrok",
                "notes":  "Two adjacent unknowns — walk past both to find context",
                "expect": {"nw": "new", "yrok": "york"},
            },
            {
                "query":  "i love hihg scool",
                "notes":  "Adjacent pair — high school",
                "expect": {"hihg": "high", "scool": "school"},
            },
            {
                "query":  "ths iz the dog",
                "notes":  "Two adjacent unknowns at start — limited left context",
                "expect": {"ths": "this", "iz": "is"},
            },
            {
                "query":  "wher iz teh dog tht i luv",
                "notes":  "Five unknowns — some adjacent, full multi-pass stress test",
                "expect": {"wher": "where", "iz": "is", "teh": "the", "tht": "that", "luv": "love"},
            },
            {
                "query":  "ths iz teh dg tht i luv",
                "notes":  "Six unknowns — maximum stress test for batch + multi-pass",
                "expect": {"ths": "this", "iz": "is", "teh": "the", "dg": "dog", "tht": "that", "luv": "love"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # N-gram detection
    # -------------------------------------------------------------------------
    "ngram_detection": {
        "description": "Bigram and trigram detection from RAM cache",
        "cases": [
            {
                "query":  "new york food near me",
                "notes":  "Should detect bigram: new york",
                "expect": {},
            },
            {
                "query":  "new york city food",
                "notes":  "Should detect trigram: new york city",
                "expect": {},
            },
            {
                "query":  "african american history month",
                "notes":  "Should detect bigram: african american",
                "expect": {},
            },
            {
                "query":  "civil rights movement history",
                "notes":  "Should detect bigram: civil rights",
                "expect": {},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # N-gram formed after correction — tests Step 6
    # -------------------------------------------------------------------------
    "ngram_after_correction": {
        "description": "Corrected words form new n-grams — tests Step 6 re-check",
        "cases": [
            {
                "query":  "new yrok city",
                "notes":  "yrok → york then Step 6 re-checks and finds trigram",
                "expect": {"yrok": "york"},
            },
            {
                "query":  "african amrican history",
                "notes":  "amrican → american then Step 6 finds bigram african american",
                "expect": {"amrican": "american"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Edge positions — start and end of query
    # -------------------------------------------------------------------------
    "edge_positions": {
        "description": "Errors at start or end — limited neighbor context",
        "cases": [
            {
                "query":  "bleu is my favorite color",
                "notes":  "Error at position 0 — no left neighbor",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "my favorite color is bleu",
                "notes":  "Error at last position — no right neighbor",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "hunrgy",
                "notes":  "Single word — no context at all",
                "expect": {"hunrgy": "hungry"},
            },
            {
                "query":  "dgo",
                "notes":  "Single misspelled word",
                "expect": {"dgo": "dog"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # All valid — no Redis call should be made
    # -------------------------------------------------------------------------
    "all_valid": {
        "description": "Clean queries — all words in RAM, Redis NOT called",
        "cases": [
            {
                "query":  "where is africa located",
                "notes":  "All words valid — no Redis",
                "expect": {},
            },
            {
                "query":  "the quick brown fox",
                "notes":  "All words valid",
                "expect": {},
            },
            {
                "query":  "african food near me",
                "notes":  "All words valid",
                "expect": {},
            },
            {
                "query":  "new york city",
                "notes":  "All words valid including proper nouns",
                "expect": {},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Stopwords only
    # -------------------------------------------------------------------------
    "stopwords": {
        "description": "Pure stopword queries — no Redis, no corrections",
        "cases": [
            {
                "query":  "the and is",
                "notes":  "Pure stopwords — all tagged from STOPWORDS dict",
                "expect": {},
            },
            {
                "query":  "i am the",
                "notes":  "Pure stopwords",
                "expect": {},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Short words
    # -------------------------------------------------------------------------
    "short_words": {
        "description": "Short words with typos — limited fuzzy candidates",
        "cases": [
            {
                "query":  "i wnt to go",
                "notes":  "3-letter word missing vowel → want",
                "expect": {"wnt": "want"},
            },
            {
                "query":  "th dog is cute",
                "notes":  "2-letter word missing vowel → the",
                "expect": {"th": "the"},
            },
            {
                "query":  "she si happy",
                "notes":  "2-letter transposition → is",
                "expect": {"si": "is"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Proper nouns and locations
    # -------------------------------------------------------------------------
    "proper_nouns": {
        "description": "Names, places, and proper nouns with typos",
        "cases": [
            {
                "query":  "i visited goergia",
                "notes":  "State name with transposition → georgia",
                "expect": {"goergia": "georgia"},
            },
            {
                "query":  "atlnta is a city",
                "notes":  "City name missing vowel → atlanta",
                "expect": {"atlnta": "atlanta"},
            },
            {
                "query":  "howard univeristy",
                "notes":  "Proper noun context → university",
                "expect": {"univeristy": "university"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Punctuation handling
    # -------------------------------------------------------------------------
    "punctuation": {
        "description": "Words with attached punctuation marks",
        "cases": [
            {
                "query":  "is the dog bleu?",
                "notes":  "Error before question mark",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "the dog is bleu, not red",
                "notes":  "Error before comma",
                "expect": {"bleu": "blue"},
            },
            {
                "query":  "i love music!",
                "notes":  "Valid sentence with exclamation — no correction",
                "expect": {},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Suffix-based POS refinement — tests Step 3.5
    # -------------------------------------------------------------------------
    "suffix_pos": {
        "description": "Suffix detection refines POS prediction in Step 3.5",
        "cases": [
            {
                "query":  "she is very happly",
                "notes":  "Suffix -ly → adverb prediction → happily",
                "expect": {"happly": "happily"},
            },
            {
                "query":  "the importnt meeting",
                "notes":  "Suffix -ant → adjective prediction → important",
                "expect": {"importnt": "important"},
            },
            {
                "query":  "she was very successfl",
                "notes":  "Suffix -ful → adjective → successful",
                "expect": {"successfl": "successful"},
            },
        ]
    },

    # -------------------------------------------------------------------------
    # Mixed content — numbers in query
    # -------------------------------------------------------------------------
    "mixed_content": {
        "description": "Sentences with numbers or mixed content",
        "cases": [
            {
                "query":  "i have 3 dgs",
                "notes":  "Number followed by typo → dogs",
                "expect": {"dgs": "dogs"},
            },
            {
                "query":  "the year 1965 was importnt",
                "notes":  "Typo after a year number → important",
                "expect": {"importnt": "important"},
            },
        ]
    },
}


# =============================================================================
# HELPERS
# =============================================================================

def _check_adjacent_unknowns(terms: list) -> bool:
    """Return True if any two consecutive words are both unknown or corrected."""
    for i in range(len(terms) - 1):
        a = terms[i]["status"] in ("unknown", "corrected")
        b = terms[i + 1]["status"] in ("unknown", "corrected")
        if a and b:
            return True
    return False


def _validate_expectations(expect: dict, corrections: list) -> dict:
    """Check if expected corrections were made. Returns pass/fail per word."""
    correction_map = {c["original"]: c["corrected"] for c in corrections}
    results = {}

    if not expect:
        results["__no_corrections_expected__"] = len(corrections) == 0
        return results

    for original, expected_corrected in expect.items():
        if original not in correction_map:
            results[original] = {
                "passed":   False,
                "expected": expected_corrected,
                "got":      None,
                "note":     "Word was not corrected",
            }
        elif correction_map[original].lower() != expected_corrected.lower():
            results[original] = {
                "passed":   False,
                "expected": expected_corrected,
                "got":      correction_map[original],
                "note":     "Wrong correction",
            }
        else:
            results[original] = {
                "passed":   True,
                "expected": expected_corrected,
                "got":      correction_map[original],
            }

    return results


def _build_word_debug(term: dict) -> dict:
    """Build detailed debug entry for a single word position."""
    entry = {
        "position":      term["position"],
        "array_index":   term["position"] - 1,
        "word":          term["word"],
        "display":       term.get("display", term["word"]),
        "status":        term["status"],
        "pos":           term.get("pos", "unknown"),
        "predicted_pos": term.get("predicted_pos"),
        "ram": {
            "found":       term["status"] not in ("unknown",),
            "is_stopword": term["is_stopword"],
            "match_count": term.get("match_count", 0),
            "category":    term.get("category", ""),
            "rank":        term.get("rank", 0),
            "entity_type": term.get("entity_type", ""),
        },
        "ngram": {
            "part_of_ngram": term.get("part_of_ngram", False),
        },
        "redis_correction": None,
    }

    if term["status"] in ("corrected", "pos_corrected"):
        entry["redis_correction"] = {
            "original":          term["word"],
            "corrected":         term.get("corrected"),
            "corrected_display": term.get("corrected_display"),
            "edit_distance":     term.get("distance"),
            "correction_type":   term["status"],
        }

    return entry


def _build_ngram_debug(ngram: dict) -> dict:
    """Build detailed debug entry for a detected n-gram."""
    return {
        "type":            ngram["type"],
        "words":           ngram["words"],
        "phrase":          ngram["phrase"],
        "display":         ngram["display"],
        "positions":       ngram["positions"],
        "category":        ngram["category"],
        "pos":             ngram["pos"],
        "rank":            ngram["rank"],
        "from_correction": ngram.get("from_correction", False),
    }


def _process_single_query(wd_engine: WordDiscovery, query: str, expect: dict = None) -> dict:
    """Process one query and return full debug response."""
    q_start = time.perf_counter()

    try:
        output = wd_engine.process(query)
    except Exception as e:
        return {"query": query, "error": str(e), "status": "error"}

    q_elapsed   = (time.perf_counter() - q_start) * 1000
    terms       = output["terms"]
    ngrams      = output.get("ngrams", [])
    corrections = output.get("corrections", [])
    stats       = output["stats"]

    words_debug  = [_build_word_debug(t) for t in terms]
    ngrams_debug = [_build_ngram_debug(n) for n in ngrams]

    # Redis batch summary
    redis_words = [
        {
            "position":      t["position"],
            "original_word": t["word"],
            "predicted_pos": t.get("predicted_pos"),
            "corrected_to":  t.get("corrected"),
            "edit_distance": t.get("distance"),
            "status":        t["status"],
        }
        for t in terms if t["status"] in ("corrected", "pos_corrected")
    ]

    unknown_words = [
        {
            "position":      t["position"],
            "word":          t["word"],
            "predicted_pos": t.get("predicted_pos"),
        }
        for t in terms if t["status"] == "unknown"
    ]

    total_redis = len(redis_words) + len(unknown_words)

    redis_summary = {
        "batch_was_used":      total_redis > 0,
        "words_sent_to_redis": total_redis,
        "words_corrected":     len(redis_words),
        "words_still_unknown": len(unknown_words),
        "batch_details":       redis_words,
        "still_unknown":       unknown_words,
        "note": (
            f"All {total_redis} unknown words sent to Redis in ONE pipeline batch call"
            if total_redis > 1 else
            "Single word sent to Redis"
            if total_redis == 1 else
            "No Redis call needed — all words found in RAM"
        ),
    }

    # POS pattern across the full query
    pos_pattern = " → ".join(
        f"[{t['pos'] or t.get('predicted_pos', '?')}]"
        for t in terms
    )

    pos_detail = [
        {
            "position":      t["position"],
            "word":          t["word"],
            "pos":           t.get("pos", "unknown"),
            "predicted_pos": t.get("predicted_pos"),
            "matched_from": (
                "stopword_dict"    if t["is_stopword"] else
                "ram_exact_match"  if t["status"] == "valid" else
                "redis_correction" if t["status"] in ("corrected", "pos_corrected") else
                "unknown"
            ),
        }
        for t in terms
    ]

    # Edge case flags
    edge_cases = {
        "has_misspelled_words":             stats["corrected_words"] > 0,
        "has_unknown_words":                stats["unknown_words"] > 0,
        "has_multiple_unknowns":            stats["unknown_words"] > 1,
        "has_adjacent_unknowns":            _check_adjacent_unknowns(terms),
        "has_ngrams":                       stats["ngram_count"] > 0,
        "has_bigrams":                      any(n["type"] == "bigram"   for n in ngrams),
        "has_trigrams":                     any(n["type"] == "trigram"  for n in ngrams),
        "has_quadgrams":                    any(n["type"] == "quadgram" for n in ngrams),
        "all_stopwords":                    stats["stopwords"] == stats["total_words"],
        "single_word_query":                stats["total_words"] == 1,
        "redis_batch_triggered":            total_redis > 1,
        "ngram_formed_after_correction":    any(n.get("from_correction") for n in ngrams),
    }

    # Validation against expected corrections
    validation = None
    if expect is not None:
        validation_raw = _validate_expectations(expect, corrections)
        all_passed = all(
            v if isinstance(v, bool) else v.get("passed", False)
            for v in validation_raw.values()
        )
        validation = {
            "passed":  all_passed,
            "details": validation_raw,
        }

    result = {
        "query":           query,
        "corrected_query": output["corrected_query"],
        "status":          "ok",
        "timing": {
            "total_ms":    round(q_elapsed, 2),
            "reported_ms": output["processing_time_ms"],
        },
        "stats":       stats,
        "edge_cases":  edge_cases,
        "pos_pattern": pos_pattern,
        "pos_detail":  pos_detail,
        "words":       words_debug,
        "ngrams":      ngrams_debug,
        "redis":       redis_summary,
        "corrections": corrections,
    }

    if validation is not None:
        result["validation"] = validation

    return result


# =============================================================================
# MAIN DEBUG VIEW
# =============================================================================

@csrf_exempt
@require_http_methods(["GET"])
def debug_word_discovery(request):
    """
    Debug endpoint for WordDiscovery pipeline.

    GET /debug/word-discovery/?q=your+query
    GET /debug/word-discovery/?category=keyboard_typos
    GET /debug/word-discovery/?run_all=1
    GET /debug/word-discovery/?q=bleu+dog&run_all=1
    """

    query    = request.GET.get("q", "").strip()
    category = request.GET.get("category", "").strip()
    run_all  = request.GET.get("run_all", "0") == "1"

    ram_loaded = (
        RAM_CACHE_AVAILABLE
        and vocab_cache is not None
        and getattr(vocab_cache, "loaded", False)
    )

    system_status = {
        "ram_cache_available": RAM_CACHE_AVAILABLE,
        "ram_cache_loaded":    ram_loaded,
        "ram_word_count":      getattr(vocab_cache, "word_count", 0) if ram_loaded else 0,
        "stopword_count":      len(STOPWORDS),
    }

    if not query and not run_all and not category:
        return JsonResponse({
            "error": "No query provided.",
            "usage": {
                "single_query":   "/debug/word-discovery/?q=where+iz+teh+dog",
                "by_category":    "/debug/word-discovery/?category=keyboard_typos",
                "run_all":        "/debug/word-discovery/?run_all=1",
                "query_plus_all": "/debug/word-discovery/?q=my+query&run_all=1",
            },
            "available_categories": {
                k: {
                    "description": v["description"],
                    "case_count":  len(v["cases"]),
                }
                for k, v in TEST_CASES.items()
            },
            "system": system_status,
        }, status=400)

    wd_engine   = WordDiscovery(verbose=False)
    total_start = time.perf_counter()

    # -------------------------------------------------------------------------
    # Single user-supplied query
    # -------------------------------------------------------------------------
    if query and not run_all and not category:
        result = _process_single_query(wd_engine, query, expect=None)
        total_elapsed = (time.perf_counter() - total_start) * 1000
        return JsonResponse({
            "system":        system_status,
            "total_time_ms": round(total_elapsed, 2),
            "result":        result,
        }, json_dumps_params={"indent": 2})

    # -------------------------------------------------------------------------
    # Specific category
    # -------------------------------------------------------------------------
    if category:
        if category not in TEST_CASES:
            return JsonResponse({
                "error":                f"Unknown category: '{category}'",
                "available_categories": list(TEST_CASES.keys()),
            }, status=400)

        cat_data   = TEST_CASES[category]
        cat_results = []
        passed = failed = 0

        # Run user query first if provided
        if query:
            cat_results.append(_process_single_query(wd_engine, query, expect=None))

        for case in cat_data["cases"]:
            r = _process_single_query(wd_engine, case["query"], expect=case.get("expect"))
            r["test_notes"] = case.get("notes", "")
            cat_results.append(r)
            if r.get("validation"):
                if r["validation"]["passed"]:
                    passed += 1
                else:
                    failed += 1

        total_elapsed = (time.perf_counter() - total_start) * 1000
        return JsonResponse({
            "system":        system_status,
            "category":      category,
            "description":   cat_data["description"],
            "total_time_ms": round(total_elapsed, 2),
            "summary": {
                "total":  len(cat_data["cases"]),
                "passed": passed,
                "failed": failed,
            },
            "results": cat_results,
        }, json_dumps_params={"indent": 2})

    # -------------------------------------------------------------------------
    # Run ALL categories
    # -------------------------------------------------------------------------
    if run_all:
        all_results  = {}
        total_passed = total_failed = 0

        # Run user query first if provided
        user_result = None
        if query:
            user_result = _process_single_query(wd_engine, query, expect=None)

        for cat_key, cat_data in TEST_CASES.items():
            cat_results = []
            cat_passed = cat_failed = 0

            for case in cat_data["cases"]:
                r = _process_single_query(wd_engine, case["query"], expect=case.get("expect"))
                r["test_notes"] = case.get("notes", "")
                cat_results.append(r)
                if r.get("validation"):
                    if r["validation"]["passed"]:
                        cat_passed += 1
                        total_passed += 1
                    else:
                        cat_failed += 1
                        total_failed += 1

            all_results[cat_key] = {
                "description": cat_data["description"],
                "summary": {
                    "total":  len(cat_results),
                    "passed": cat_passed,
                    "failed": cat_failed,
                },
                "results": cat_results,
            }

        total_elapsed = (time.perf_counter() - total_start) * 1000

        response = {
            "system":        system_status,
            "total_time_ms": round(total_elapsed, 2),
            "overall_summary": {
                "categories":  len(TEST_CASES),
                "total_cases": total_passed + total_failed,
                "passed":      total_passed,
                "failed":      total_failed,
            },
            "categories": all_results,
        }

        if user_result:
            response["user_query_result"] = user_result

        return JsonResponse(response, json_dumps_params={"indent": 2})