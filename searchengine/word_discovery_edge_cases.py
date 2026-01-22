"""
test_correction_edge_cases.py
Test script for spelling correction edge cases and error scenarios.

Run this script to validate the correction system handles various edge cases.

USAGE:
    python test_correction_edge_cases.py
    python test_correction_edge_cases.py --verbose
    python test_correction_edge_cases.py --category keyboard
"""

import sys
import time
from typing import Dict, Any, List, Optional

# =============================================================================
# TEST CASE DEFINITIONS
# =============================================================================

TEST_CASES = {
    # -------------------------------------------------------------------------
    # CATEGORY: Simple Single Typos
    # -------------------------------------------------------------------------
    "simple_typos": {
        "description": "Basic single character errors",
        "cases": [
            {
                "input": "the dog is bleu",
                "expected_correction": "blue",
                "position": 4,
                "notes": "Simple substitution: e/u swap"
            },
            {
                "input": "she is hunrgy",
                "expected_correction": "hungry",
                "position": 3,
                "notes": "Transposition: r and g"
            },
            {
                "input": "I went too the store",
                "expected_correction": None,  # "too" is valid, context should help
                "position": None,
                "notes": "Valid word but wrong context - may not correct"
            },
            {
                "input": "the quik brown fox",
                "expected_correction": "quick",
                "position": 2,
                "notes": "Missing letter: c"
            },
            {
                "input": "she recieved a gift",
                "expected_correction": "received",
                "position": 2,
                "notes": "Common misspelling: ie vs ei"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Keyboard Adjacent Typos
    # -------------------------------------------------------------------------
    "keyboard_typos": {
        "description": "Errors from hitting adjacent keys",
        "cases": [
            {
                "input": "thr dog is brown",
                "expected_correction": "the",
                "position": 1,
                "notes": "r is adjacent to e on keyboard"
            },
            {
                "input": "the cat ir cute",
                "expected_correction": "is",
                "position": 3,
                "notes": "r is adjacent to s on keyboard"
            },
            {
                "input": "i luv music",
                "expected_correction": "love",
                "position": 2,
                "notes": "u is adjacent to i, missing o"
            },
            {
                "input": "thw quick brown fox",
                "expected_correction": "the",
                "position": 1,
                "notes": "w is adjacent to e"
            },
            {
                "input": "school iz fun",
                "expected_correction": "is",
                "position": 2,
                "notes": "z is adjacent to s"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: POS Disambiguation
    # -------------------------------------------------------------------------
    "pos_disambiguation": {
        "description": "Cases where POS should help choose the right correction",
        "cases": [
            {
                "input": "the sky is bleu today",
                "expected_correction": "blue",
                "position": 4,
                "notes": "Context: [be] ___ [noun] → adjective, not 'blew' (verb)"
            },
            {
                "input": "the wind bleu hard",
                "expected_correction": "blew",
                "position": 3,
                "notes": "Context: [noun] ___ [adverb] → verb, not 'blue' (adjective)"
            },
            {
                "input": "i red the book",
                "expected_correction": "read",
                "position": 2,
                "notes": "Context: [pronoun] ___ [article] → verb, not 'red' (adjective)"
            },
            {
                "input": "the red car",
                "expected_correction": None,
                "position": None,
                "notes": "'red' is valid adjective here - no correction needed"
            },
            {
                "input": "she through the ball",
                "expected_correction": "threw",
                "position": 2,
                "notes": "Context: [pronoun] ___ [article] → verb"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Multiple Errors
    # -------------------------------------------------------------------------
    "multiple_errors": {
        "description": "Sentences with multiple typos",
        "cases": [
            {
                "input": "teh dgo is bleu",
                "expected_corrections": {"teh": "the", "dgo": "dog", "bleu": "blue"},
                "notes": "Three errors in one sentence"
            },
            {
                "input": "she iz vrey hunrgy",
                "expected_corrections": {"iz": "is", "vrey": "very", "hunrgy": "hungry"},
                "notes": "Three errors: keyboard, transposition, transposition"
            },
            {
                "input": "i cant wayt too see yuo",
                "expected_corrections": {"wayt": "wait", "yuo": "you"},
                "notes": "Multiple errors, some words valid (cant, too)"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Adjacent Unknown Pairs (Bigram Correction)
    # -------------------------------------------------------------------------
    "adjacent_pairs": {
        "description": "Two consecutive unknowns that form a known bigram",
        "cases": [
            {
                "input": "i visited nw yrok",
                "expected_corrections": {"nw": "new", "yrok": "york"},
                "notes": "Should correct as bigram 'new york'"
            },
            {
                "input": "the ws angeles lakers",
                "expected_corrections": {"ws": "los", "angeles": "angeles"},
                "notes": "Should correct as bigram 'los angeles'"
            },
            {
                "input": "i love hihg scool",
                "expected_corrections": {"hihg": "high", "scool": "school"},
                "notes": "Should correct as bigram 'high school'"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Compound Words (Split/Merge)
    # -------------------------------------------------------------------------
    "compound_words": {
        "description": "Words that should be split or merged",
        "cases": [
            {
                "input": "i love newyork",
                "expected_split": ["new", "york"],
                "notes": "Missing space - should split"
            },
            {
                "input": "she plays base ball",
                "expected_merge": "baseball",
                "notes": "Extra space - should merge"
            },
            {
                "input": "i visited losangeles",
                "expected_split": ["los", "angeles"],
                "notes": "Missing space - should split"
            },
            {
                "input": "the black board is clean",
                "expected_merge": "blackboard",
                "notes": "Extra space - should merge (if blackboard is in vocab)"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Bigram Context Boost
    # -------------------------------------------------------------------------
    "bigram_context": {
        "description": "Corrections boosted by forming known bigrams",
        "cases": [
            {
                "input": "new yrok city",
                "expected_correction": "york",
                "position": 2,
                "notes": "'new' + candidate should boost 'york' over other options"
            },
            {
                "input": "african amrican history",
                "expected_correction": "american",
                "position": 2,
                "notes": "'african' + candidate should boost 'american'"
            },
            {
                "input": "civil rihgts movement",
                "expected_correction": "rights",
                "position": 2,
                "notes": "'civil' + candidate should boost 'rights'"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Edge Positions (Start/End of Sentence)
    # -------------------------------------------------------------------------
    "edge_positions": {
        "description": "Errors at start or end with limited context",
        "cases": [
            {
                "input": "bleu is my favorite color",
                "expected_correction": "blue",
                "position": 1,
                "notes": "Error at position 1 - no left context"
            },
            {
                "input": "my favorite color is bleu",
                "expected_correction": "blue",
                "position": 5,
                "notes": "Error at last position - no right context"
            },
            {
                "input": "hunrgy",
                "expected_correction": "hungry",
                "position": 1,
                "notes": "Single word query - no context at all"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: All Valid (No Corrections)
    # -------------------------------------------------------------------------
    "all_valid": {
        "description": "Sentences with no errors - should pass through unchanged",
        "cases": [
            {
                "input": "the quick brown fox",
                "expected_corrections": {},
                "notes": "All words valid"
            },
            {
                "input": "she is very happy today",
                "expected_corrections": {},
                "notes": "All words valid"
            },
            {
                "input": "new york city",
                "expected_corrections": {},
                "notes": "All words valid including proper nouns"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Short Words
    # -------------------------------------------------------------------------
    "short_words": {
        "description": "Short words are harder to correct (fewer candidates)",
        "cases": [
            {
                "input": "i wnt to go",
                "expected_correction": "want",
                "position": 2,
                "notes": "3-letter word missing vowel"
            },
            {
                "input": "th dog is cute",
                "expected_correction": "the",
                "position": 1,
                "notes": "2-letter word missing vowel"
            },
            {
                "input": "she si happy",
                "expected_correction": "is",
                "position": 2,
                "notes": "2-letter transposition"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Proper Nouns / Locations
    # -------------------------------------------------------------------------
    "proper_nouns": {
        "description": "Names, places, and proper nouns",
        "cases": [
            {
                "input": "i visited goergia",
                "expected_correction": "georgia",
                "position": 3,
                "notes": "State name with transposition"
            },
            {
                "input": "atlnta is a city",
                "expected_correction": "atlanta",
                "position": 1,
                "notes": "City name missing vowel"
            },
            {
                "input": "howard univeristy",
                "expected_correction": "university",
                "position": 2,
                "notes": "Common misspelling in proper noun context"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Punctuation Handling
    # -------------------------------------------------------------------------
    "punctuation": {
        "description": "Words with attached punctuation",
        "cases": [
            {
                "input": "is the dog bleu?",
                "expected_correction": "blue",
                "position": 4,
                "notes": "Error before question mark"
            },
            {
                "input": "the dog is bleu, not red",
                "expected_correction": "blue",
                "position": 4,
                "notes": "Error before comma"
            },
            {
                "input": "i love music!",
                "expected_corrections": {},
                "notes": "Valid sentence with exclamation - no changes"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Numbers and Mixed Content
    # -------------------------------------------------------------------------
    "mixed_content": {
        "description": "Sentences with numbers or mixed content",
        "cases": [
            {
                "input": "i have 3 dgs",
                "expected_correction": "dogs",
                "position": 4,
                "notes": "Number followed by typo"
            },
            {
                "input": "the year 1965 was importnt",
                "expected_correction": "important",
                "position": 5,
                "notes": "Typo after year"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Homophones
    # -------------------------------------------------------------------------
    "homophones": {
        "description": "Words that sound alike but have different meanings",
        "cases": [
            {
                "input": "i want to by a car",
                "expected_correction": "buy",
                "position": 4,
                "notes": "by vs buy - context: 'to ___' suggests verb"
            },
            {
                "input": "their going to the store",
                "expected_correction": "they're",
                "position": 1,
                "notes": "their vs they're - context: '___ going' suggests pronoun+verb"
            },
            {
                "input": "the car is over they're",
                "expected_correction": "there",
                "position": 5,
                "notes": "they're vs there - context: 'over ___' suggests location"
            },
        ]
    },
    
    # -------------------------------------------------------------------------
    # CATEGORY: Repeated Letters
    # -------------------------------------------------------------------------
    "repeated_letters": {
        "description": "Extra or missing repeated letters",
        "cases": [
            {
                "input": "i am happppy",
                "expected_correction": "happy",
                "position": 3,
                "notes": "Too many p's"
            },
            {
                "input": "the ballon is red",
                "expected_correction": "balloon",
                "position": 2,
                "notes": "Missing repeated l"
            },
            {
                "input": "she is sucessful",
                "expected_correction": "successful",
                "position": 3,
                "notes": "Missing repeated c and s"
            },
        ]
    },
}


# =============================================================================
# TEST RUNNER
# =============================================================================

class TestRunner:
    """Runs edge case tests against the correction system."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results = {
            'passed': 0,
            'failed': 0,
            'skipped': 0,
            'errors': 0,
        }
        self.failures = []
        self.errors = []
        
        # Try to import the correction system
        self.system_available = self._import_system()
    
    def _import_system(self) -> bool:
        """Import the word discovery system."""
        try:
            # Try relative import first
            from word_discovery import process_query_optimized, get_corrections
            self.process_query = process_query_optimized
            self.get_corrections = get_corrections
            return True
        except ImportError:
            try:
                # Try absolute import
                import word_discovery
                self.process_query = word_discovery.process_query_optimized
                self.get_corrections = word_discovery.get_corrections
                return True
            except ImportError:
                print("WARNING: Could not import word_discovery module")
                print("Tests will run in mock mode (structure validation only)")
                return False
    
    def run_all_tests(self, categories: Optional[List[str]] = None):
        """Run all test categories or specified categories."""
        print("=" * 70)
        print("SPELLING CORRECTION EDGE CASE TESTS")
        print("=" * 70)
        
        if not self.system_available:
            print("\n⚠️  Running in MOCK MODE - structure validation only\n")
        
        categories_to_run = categories or list(TEST_CASES.keys())
        
        for category in categories_to_run:
            if category not in TEST_CASES:
                print(f"\n⚠️  Unknown category: {category}")
                continue
            
            self._run_category(category, TEST_CASES[category])
        
        self._print_summary()
    
    def _run_category(self, category: str, category_data: Dict):
        """Run all tests in a category."""
        print(f"\n{'─' * 70}")
        print(f"CATEGORY: {category}")
        print(f"Description: {category_data['description']}")
        print(f"{'─' * 70}")
        
        for i, test_case in enumerate(category_data['cases'], 1):
            self._run_test(category, i, test_case)
    
    def _run_test(self, category: str, test_num: int, test_case: Dict):
        """Run a single test case."""
        input_text = test_case['input']
        notes = test_case.get('notes', '')
        
        if self.verbose:
            print(f"\n  Test {test_num}: \"{input_text}\"")
            print(f"    Notes: {notes}")
        
        try:
            if self.system_available:
                # Run actual test
                result = self.process_query(input_text, verbose=False)
                passed = self._validate_result(test_case, result)
            else:
                # Mock mode - just validate test case structure
                passed = self._validate_test_structure(test_case)
            
            if passed:
                self.results['passed'] += 1
                status = "✓ PASS"
            else:
                self.results['failed'] += 1
                status = "✗ FAIL"
                self.failures.append({
                    'category': category,
                    'test': test_num,
                    'input': input_text,
                    'notes': notes
                })
            
            if self.verbose or not passed:
                print(f"  [{status}] \"{input_text}\"")
                if not passed and self.system_available:
                    corrections = result.get('corrections', [])
                    print(f"    Got corrections: {corrections}")
        
        except Exception as e:
            self.results['errors'] += 1
            self.errors.append({
                'category': category,
                'test': test_num,
                'input': input_text,
                'error': str(e)
            })
            print(f"  [ERROR] \"{input_text}\": {e}")
    
    def _validate_result(self, test_case: Dict, result: Dict) -> bool:
        """Validate the correction result against expected values."""
        corrections = result.get('corrections', [])
        correction_map = {c['original']: c['corrected'] for c in corrections}
        
        # Single expected correction
        if 'expected_correction' in test_case:
            expected = test_case['expected_correction']
            position = test_case.get('position')
            
            if expected is None:
                # Expect no correction at this position
                return len(corrections) == 0 or position not in [c.get('position') for c in corrections]
            
            # Find if the expected correction was made
            for corr in corrections:
                if corr.get('corrected', '').lower() == expected.lower():
                    return True
            return False
        
        # Multiple expected corrections
        if 'expected_corrections' in test_case:
            expected = test_case['expected_corrections']
            
            if not expected:
                # Expect no corrections
                return len(corrections) == 0
            
            # Check each expected correction
            for original, expected_corrected in expected.items():
                if original not in correction_map:
                    return False
                if correction_map[original].lower() != expected_corrected.lower():
                    return False
            return True
        
        # Split expectation
        if 'expected_split' in test_case:
            # Check if the word was split correctly
            expected_parts = test_case['expected_split']
            corrected_query = result.get('corrected_query', '')
            return all(part in corrected_query.lower() for part in expected_parts)
        
        # Merge expectation
        if 'expected_merge' in test_case:
            expected_merged = test_case['expected_merge']
            corrected_query = result.get('corrected_query', '')
            return expected_merged.lower() in corrected_query.lower()
        
        return True  # No specific expectation
    
    def _validate_test_structure(self, test_case: Dict) -> bool:
        """Validate test case structure (mock mode)."""
        required = ['input']
        return all(key in test_case for key in required)
    
    def _print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        total = sum(self.results.values())
        print(f"  Total:   {total}")
        print(f"  Passed:  {self.results['passed']} ✓")
        print(f"  Failed:  {self.results['failed']} ✗")
        print(f"  Errors:  {self.results['errors']} ⚠")
        
        if self.failures:
            print(f"\n{'─' * 70}")
            print("FAILURES:")
            for f in self.failures:
                print(f"  [{f['category']}:{f['test']}] \"{f['input']}\"")
                print(f"    Notes: {f['notes']}")
        
        if self.errors:
            print(f"\n{'─' * 70}")
            print("ERRORS:")
            for e in self.errors:
                print(f"  [{e['category']}:{e['test']}] \"{e['input']}\"")
                print(f"    Error: {e['error']}")
        
        print("=" * 70)
        
        # Exit code
        if self.results['failed'] == 0 and self.results['errors'] == 0:
            print("All tests passed! ✓")
            return 0
        else:
            print("Some tests failed or had errors ✗")
            return 1


# =============================================================================
# INTERACTIVE TEST RUNNER
# =============================================================================

def run_interactive():
    """Run tests interactively, allowing user to input sentences."""
    print("=" * 70)
    print("INTERACTIVE SPELLING CORRECTION TEST")
    print("=" * 70)
    print("Enter sentences to test. Type 'quit' to exit.\n")
    
    try:
        from word_discovery import process_query_optimized
    except ImportError:
        print("ERROR: Could not import word_discovery module")
        return
    
    while True:
        try:
            user_input = input("\nEnter sentence: ").strip()
            
            if user_input.lower() in ('quit', 'exit', 'q'):
                break
            
            if not user_input:
                continue
            
            print("\nProcessing...")
            result = process_query_optimized(user_input, verbose=True)
            
            print(f"\n{'─' * 50}")
            print(f"Original:  {result.get('query', '')}")
            print(f"Corrected: {result.get('corrected_query', '')}")
            print(f"Time:      {result.get('processing_time_ms', 0):.2f}ms")
            
            corrections = result.get('corrections', [])
            if corrections:
                print(f"\nCorrections made:")
                for c in corrections:
                    print(f"  '{c['original']}' → '{c['corrected']}' "
                          f"(dist={c.get('distance', '?')}, "
                          f"score={c.get('score', '?')})")
            else:
                print("\nNo corrections needed.")
        
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


# =============================================================================
# BENCHMARK RUNNER
# =============================================================================

def run_benchmark(iterations: int = 100):
    """Run performance benchmark on test cases."""
    print("=" * 70)
    print(f"PERFORMANCE BENCHMARK ({iterations} iterations)")
    print("=" * 70)
    
    try:
        from word_discovery import process_query_optimized
    except ImportError:
        print("ERROR: Could not import word_discovery module")
        return
    
    # Collect all test inputs
    all_inputs = []
    for category_data in TEST_CASES.values():
        for case in category_data['cases']:
            all_inputs.append(case['input'])
    
    print(f"Testing {len(all_inputs)} sentences × {iterations} iterations")
    print(f"Total operations: {len(all_inputs) * iterations}\n")
    
    # Warm up
    print("Warming up...")
    for inp in all_inputs[:5]:
        process_query_optimized(inp, verbose=False)
    
    # Benchmark
    print("Running benchmark...")
    start_time = time.perf_counter()
    
    for _ in range(iterations):
        for inp in all_inputs:
            process_query_optimized(inp, verbose=False)
    
    elapsed = time.perf_counter() - start_time
    total_ops = len(all_inputs) * iterations
    
    print(f"\n{'─' * 50}")
    print(f"Total time:     {elapsed:.2f}s")
    print(f"Ops/second:     {total_ops / elapsed:.1f}")
    print(f"Avg per query:  {(elapsed / total_ops) * 1000:.2f}ms")
    print("=" * 70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test spelling correction edge cases"
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help="Show verbose output for each test"
    )
    parser.add_argument(
        '--category', '-c',
        type=str,
        help="Run only specific category (comma-separated for multiple)"
    )
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help="List available test categories"
    )
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help="Run interactive test mode"
    )
    parser.add_argument(
        '--benchmark', '-b',
        action='store_true',
        help="Run performance benchmark"
    )
    parser.add_argument(
        '--iterations',
        type=int,
        default=100,
        help="Number of iterations for benchmark (default: 100)"
    )
    
    args = parser.parse_args()
    
    if args.list:
        print("Available test categories:")
        for cat, data in TEST_CASES.items():
            print(f"  {cat}: {data['description']} ({len(data['cases'])} tests)")
        return 0
    
    if args.interactive:
        run_interactive()
        return 0
    
    if args.benchmark:
        run_benchmark(args.iterations)
        return 0
    
    # Parse categories
    categories = None
    if args.category:
        categories = [c.strip() for c in args.category.split(',')]
    
    # Run tests
    runner = TestRunner(verbose=args.verbose)
    return runner.run_all_tests(categories)


if __name__ == "__main__":
    sys.exit(main() or 0)