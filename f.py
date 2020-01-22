from conftest import config, standup, action


@config(params = {'A': 'A', 'B': 'B'})
def outer(request):
    return f'outer-{request.param}'


@config(params = {'a': 'a', 'b': 'b'})
def inner(request):
    return f'inner-{request.param}'


@standup
def s(test_dir, outer, inner):
    (test_dir / outer).touch()
    (test_dir / inner).touch()


@action
def a(s):
    pass


class TestFoobar:
    def test_foobar(self, a):
        assert True


class TestWizbang:
    def test_wizbang(self, a):
        assert True
