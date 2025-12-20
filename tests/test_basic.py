"""
Basic tests for extraction scripts

Run with: python tests/test_basic.py
Or with pytest: pytest tests/
"""

import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))


def test_imports():
    """Test that all scripts can be imported"""
    try:
        import extract_pums_distributions
        import extract_bls_distributions
        import extract_derived_distributions
        print("✅ All scripts imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False


def test_directories_exist():
    """Test that required directories exist"""
    repo_root = Path(__file__).parent.parent
    
    required_dirs = [
        repo_root / 'cache',
        repo_root / 'output',
        repo_root / 'scripts',
        repo_root / 'docs',
        repo_root / 'database',
    ]
    
    all_exist = True
    for directory in required_dirs:
        if directory.exists():
            print(f"✅ Directory exists: {directory.name}/")
        else:
            print(f"❌ Directory missing: {directory.name}/")
            all_exist = False
    
    return all_exist


def test_config_files_exist():
    """Test that configuration files exist"""
    repo_root = Path(__file__).parent.parent
    
    required_files = [
        repo_root / 'requirements.txt',
        repo_root / 'README.md',
        repo_root / '.gitignore',
        repo_root / '.env.example',
    ]
    
    all_exist = True
    for file_path in required_files:
        if file_path.exists():
            print(f"✅ File exists: {file_path.name}")
        else:
            print(f"❌ File missing: {file_path.name}")
            all_exist = False
    
    return all_exist


def test_scripts_executable():
    """Test that scripts are present and readable"""
    scripts_dir = Path(__file__).parent.parent / 'scripts'
    
    required_scripts = [
        'extract_pums_distributions.py',
        'extract_bls_distributions.py',
        'extract_derived_distributions.py',
    ]
    
    all_exist = True
    for script_name in required_scripts:
        script_path = scripts_dir / script_name
        if script_path.exists():
            print(f"✅ Script exists: {script_name}")
        else:
            print(f"❌ Script missing: {script_name}")
            all_exist = False
    
    return all_exist


def run_all_tests():
    """Run all basic tests"""
    print("="*60)
    print("Running Basic Tests")
    print("="*60)
    print()
    
    tests = [
        ("Import test", test_imports),
        ("Directory test", test_directories_exist),
        ("Config files test", test_config_files_exist),
        ("Scripts test", test_scripts_executable),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 40)
        result = test_func()
        results.append((test_name, result))
        print()
    
    print("="*60)
    print("Test Summary")
    print("="*60)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(result for _, result in results)
    
    print()
    if all_passed:
        print("🎉 All tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed")
        return 1


if __name__ == '__main__':
    exit_code = run_all_tests()
    sys.exit(exit_code)
