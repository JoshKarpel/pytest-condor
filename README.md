# pytest-condor

This is an experiment in writing a test framework for HTCondor using Python.
It piggybacks off of [pytest](https://docs.pytest.org/en/latest/), a popular Python
testing framework.

There are two components to the test framework:
1. `ornithology`, a Python package that defines a wide variety of "test helper" functionality.
    For example, `ornithology` can stand up an HTCondor instance, parse the logs of HTCondor daemons,
    or track jobs via their event logs.
1. A series of pytest `conftest.py` files which hook `ornithology` up to pytest.

## Test Structure

There are two levels of test grouping: **files** (a.k.a **modules**) and **classes**.
Necessarily, ever class must appear in a single module.

There are three kinds of things that can appear in a test module:

1. **Fixture functions**, 
    defined by the decorators `@config`, `@standup`, and `@action`.
    Only one `@standup` fixture should appear; you may have any number of the others.
1. **Assertion functions**, 
    which are methods of classes named like `TestCustomMachineResources`, where
    each assertion function has a name like `test_that_it_works`.
    (the `Test` and `test_` prefixes are how pytest discovers these).
1. Helper functions or classes. Generally, we should try to push these helpers
    into `ornithology` itself, but you may need to write a specialized helper
    for a single use case.

Assertions are (generally) extremely short functions that test some condition about
the system using Python's `assert` statement 
(e.g., `assert foo == 5` succeeds if `foo` is equal to `5`, but raises an error if it is not).
*Assertion functions should never mutate the state of the system*.

To get the system into a state you want to make an assertion about, you use fixtures.
Tests and other fixtures may declare themselves as depending on another fixture.
In this case, the return value of the parent fixture is available to the child during execution,
and the parent fixture is guaranteed to run before the child.

We support three kinds of fixtures:

1. `@config` fixtures are run *before* any HTCondor instances are started. They
    can provide snippets of HTCondor config, write files into the test directory,
    or prepare the environment.
1. `@standup` is a solitary fixture that is responsible for starting all of the
    HTCondor instances that should be alive before the "test" begins. All `@config`
    fixtures run before the `@standup` fixture, even if they are not explicitly
    declared to be parents of the `@standup` fixture. You'll often do nothing more
    in a `@standup` fixture than create an `ornithology.Condor` instance with 
    the desired configuration and yield it.
1. `@action` fixtures are run after the `@standup` fixture. These fixtures are
    where you can do things like submit jobs, query the queue, and read daemon logs.
    They should generally return values that you would like to make assertions about.

The execution flow of a test module is entirely described by the fixture and assertion
functions defined in it. pytest manages the execution; you never need to explicitly
call a fixture or assertion function.

Fixture dependencies are defined by fixture and assertion function signatures.
In the below example, we define an `@action` fixture which is a parent of an assertion.
The action itself (submitting some jobs) depends on having an HTCondor instance, so
it will in turn depend on the `@standup` fixture:

```python
from ornithology import Condor

from conftest import standup, action

@action
def jobs(condor):
    handle = condor.submit(
        description = {
            "executable": "/bin/sleep",       
            "arguments": "5",       
        },
        count = 5,
    )

    return handle

class TestClass:
    def test_number_of_jobs_is_expected(self, jobs):
        assert len(jobs) == 5
```

### Fixture and Assertion Parametrization

Both fixture and assertion functions can be *parameterized*.
The entire chain of fixtures and assertions below the parametrized function will
then run once for each of the given parameters.
This is true for each function in the chain, so parametrization can easily produce
many combinations of test conditions.
