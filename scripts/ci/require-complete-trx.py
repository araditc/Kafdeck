"""Reject missing, failed or skipped required acceptance scenarios, not just a zero exit code."""
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
ns = {'t': 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}
counters = root.find('t:ResultSummary/t:Counters', ns)
assert counters is not None, 'TRX counters missing'
counts = {k: int(v) for k, v in counters.attrib.items()}
assert counts.get('total', 0) > 0 and counts.get('passed') == counts['total'], counts
assert counts.get('failed', 0) == counts.get('notExecuted', 0) == 0, counts
methods = {}
for test in root.findall('t:TestDefinitions/t:UnitTest', ns):
    method = test.find('t:TestMethod', ns)
    if method is not None: methods[test.attrib['id']] = method.attrib.get('className', '')
results = root.findall('t:Results/t:UnitTestResult', ns)
for requirement in sys.argv[2:]:
    name, minimum = requirement.rsplit(':', 1)
    actual = sum(r.attrib.get('outcome') == 'Passed' and name in methods.get(r.attrib.get('testId'), '') for r in results)
    assert actual >= int(minimum), (name, 'required', minimum, 'passed', actual)
print(f"Complete acceptance: {counts['total']} passed, zero failed/skipped; {len(sys.argv) - 2} required scenario families verified.")
