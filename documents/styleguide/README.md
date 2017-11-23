# Python Styleguide

Based on [Pocoo Styleguide](http://flask.pocoo.org/docs/0.12/styleguide/)

## General Layout

### Indentation

4 real spaces. No tabs, extend tabs to spaces.

### Maximum line length

80 Characters per line. Extend it where absolutely necessary.

### Continuing long statements

Use backslaches `\` to continue long statements.
Indent to the last equal sign like `.` or indent 4 spaces.

```python
this_is_a_very_long(function_call, 'with many parameters') \
    .that_returns_an_object_with_an_attribute

MyModel.query.filter(MyModel.scalar > 120) \
             .order_by(MyModel.name.desc()) \
             .limit(10)
```

If you break in a statement with parentheses or braces, indent to the braces:

```python
this_is_a_very_long(function_call, 'with many parameters',
                    23, 42, 'and even more')
```

For lists or tuples with many items, break immediately after the opening brace:

```python
items = [
    'this is the first', 'set of items', 'with more items',
    'to come in this line', 'like this'
]
```

### Blank lines

Top level functions and classes are separated by two lines, everything else by one. Do not use too many blank lines to separate logical segments in code.

```python
def hello(name):
    print 'Hello %s!' % name


def goodbye(name):
    print 'See you %s.' % name


class MyClass(object):
    """This is a simple docstring"""

    def __init__(self, name):
        self.name = name

    def get_annoying_name(self):
        return self.name.upper() + '!!!!111'
```

## Naming Conventions

### A Word About Describing Names

Name variables, functions/methods and classes in a way, that they describe themselve.
***This is way more important than their name length!**
Of course you should always keep them short enough to be used with other expressions in a single line.

If they become too long try to use a "known" short-form like:
```
Before:
def compare_user_and_post_id():
    #other stuff

After:
def compare_uid_pid():
    """uid: user id
    pid: post id
    """
    #other stuff
```
**Describe non standart short forms with documentation**

* Class names: CamelCase, with acronyms kept uppercase (HTTPWriter and not HttpWriter)
* Variable names: lowercase_with_underscores
* Method and function names: lowercase_with_underscores
* Private function names: __lowercase_with_leading_double_underscore
* Constants: UPPERCASE_WITH_UNDERSCORES

## Docstrings & comments

### Docstrings

**At the beginning of every function, describing it.**

All docstrings are formatted with reStructuredText as understood by Sphinx. Depending on the number of lines in the docstring, they are laid out differently. If it’s just one line, the closing triple quote is on the same line as the opening, otherwise the text is on the same line as the opening quote and the triple quote that closes the string on its own line:

```python
def foo():
    """This is a simple docstring"""


def bar():
    """This is a longer docstring with so much information in there
    that it spans three lines.  In this case the closing triple quote
    is on its own line.
    """
```

### Comments

Rules for comments are similar to docstrings. Both are formatted with reStructuredText. If a comment is used to document an attribute, put a colon after the opening pound sign (#):

```python
class User(object):
    #: the name of the user as unicode string
    name = Column(String)
    #: the sha1 hash of the password + inline salt
    pw_hash = Column(String)
```