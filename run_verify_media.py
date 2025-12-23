
import unittest
import sys
import os

# Redirect stderr to a file
with open('error_log.txt', 'w', encoding='utf-8') as f:
    runner = unittest.TextTestRunner(stream=f, verbosity=2)
    loader = unittest.TestLoader()
    suite = loader.discover('c:/roya/products/almudeer/backend/tests', pattern='verify_media.py')
    result = runner.run(suite)

print("Test run complete. Check error_log.txt")
