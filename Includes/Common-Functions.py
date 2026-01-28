# Databricks notebook source

# Simple validation function to test if operations were successful
def test_success(condition, success_message, failure_message):
    """Simple test function to validate operations."""
    if condition:
        print(f"✅ {success_message}")
        return True
    else:
        print(f"❌ {failure_message}")
        return False


# COMMAND ----------


# Simple function to create test suites
def create_test_suite(name):
    """Create a simple test suite for validating lab exercises."""
    return SimpleSuite(name)


class SimpleSuite:
    """A simplified test suite to replace DA.tests functionality."""

    def __init__(self, name):
        self.name = name
        self.tests = []
        self.passed = True

    def test(self, description, test_function):
        """Add a test with a custom test function."""
        result = test_function()
        self.tests.append((description, result))
        if not result:
            self.passed = False
        return result

    def test_equals(self, actual, expected, description):
        """Test if actual equals expected."""
        result = actual == expected
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description} - Expected {expected}, got {actual}")
        else:
            print(f"✅ {description}")
        return result

    def test_true(self, condition, description):
        """Test if condition is True."""
        result = condition == True
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description}")
        else:
            print(f"✅ {description}")
        return result

    def test_false(self, condition, description):
        """Test if condition is False."""
        result = condition == False
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(f"❌ {description}")
        else:
            print(f"✅ {description}")
        return result

    def test_length(self, collection, expected_length, description):
        """Test if collection has expected length."""
        actual_length = len(collection)
        result = actual_length == expected_length
        self.tests.append((description, result))
        if not result:
            self.passed = False
            print(
                f"❌ {description} - Expected length {expected_length}, got {actual_length}"
            )
        else:
            print(f"✅ {description}")
        return result

    def display_results(self):
        """Print test results summary."""
        total = len(self.tests)
        passed = sum(1 for _, result in self.tests if result)
        print(f"\n===== Test Results for {self.name} =====")
        print(f"Passed: {passed}/{total} tests")
        if self.passed:
            print("🎉 All tests passed!")
        else:
            print("❌ Some tests failed.")
        print("=====================================\n")
        return self.passed


# COMMAND ----------


# Simplified function to create target files folder
def reset_working_dir():
    """Reset the target files folder."""
    try:
        dbutils.fs.rm(workdir, True)
    except:
        pass

    try:
        dbutils.fs.mkdirs(workdir)
        print(f"Created empty target files folder: {workdir}")
    except:
        print(f"Failed to create target files folder: {workdir}")

