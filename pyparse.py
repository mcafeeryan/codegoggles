import sqlparse
import json

TYPES = ["SELECT"]
COLUMNS = ["Wildcard", "Identifier"]
INFIX = ["in", "or", "and", "join"]
NEST_ONE = ["from", "on"]

DEBUG = False

class TokenDuck(object):
    def __init__(self, token, tokens):
        self.token = token
        self.tokens = tokens

    def __getattr__(self, name):
        if name == "tokens":
            return self.tokens
        else:
            return getattr(self.token, name)

    def __str__(self):
        return str(self.token)

    def __repr__(self):
        return repr(self.token)

def parse_statement(sql):
    return sqlparse.parse(sql)[0]

def whitespace_filter(tokens):
    return [x for x in tokens if not x.is_whitespace()]

def punctuation_filter(tokens):
    return [x for x in tokens if not "Punctuation" in repr(x)]

def preprocess_nesting(tokens):
    on_next = False
    for idx in reversed(range(len(tokens))):
        token = tokens[idx]
        if isinstance(token, TokenDuck):
            continue
        for infix in INFIX:
            if "Keyword" in repr(tokens[idx]) and (infix == str(tokens[idx]).lower() or (infix == "join" and infix in str(tokens[idx]).lower())):
                tokens[idx] = TokenDuck(token, [tokens.pop(idx + 1), tokens.pop(idx + 1)])
                if on_next:
                    on_next = False
                    tokens[idx].tokens.append(tokens.pop(idx + 1))
        for nest_one in NEST_ONE:
            if "Keyword" in repr(token) and nest_one in str(token).lower():
                tokens[idx] = TokenDuck(token, [tokens.pop(idx + 1)])
                if nest_one == "on":
                    on_next = True
    return tokens


def create_dict(stmt, intermediate, preprocess=True):
    if DEBUG:
        print stmt
    tokens = stmt.tokens
    if preprocess:
        tokens = whitespace_filter(tokens)
        tokens = punctuation_filter(tokens)
        tokens = preprocess_infix(tokens)
        tokens = preprocess_nesting(tokens)
    while tokens:
        token = tokens.pop(0)
        parse_token(tokens, token, intermediate)
    return intermediate

def make_column(token):
    return {"type": "column", "name": token.to_unicode()}

def make_table(token):
    return {"type": "table", "name": token.to_unicode()}

def make_literal(token):
    return {"type": "literal", "name": token.to_unicode()}

def preprocess_infix(tokens):
    for infix in INFIX:
        for idx in range(1, len(tokens) - 1):
            if "Keyword" in repr(tokens[idx]) and (infix == str(tokens[idx]).lower() or (infix == "join" and infix in str(tokens[idx]).lower())):
                tokens[idx - 1], tokens[idx] = tokens[idx], tokens[idx - 1]
    return tokens

def parse_relation(token):
    if "Comparison" in repr(token):
        intermediate = {}
        comparison = whitespace_filter(token.tokens)
        intermediate['relation'] = {"type": comparison[1].to_unicode(), "items": [make_literal(comparison[0]), make_literal(comparison[2])]}
        return intermediate
    else:
        return create_dict(token, {})

def parse_token(tokens, token, intermediate):
    if DEBUG:
        print "token:", token
    for column in COLUMNS:
        if column in repr(token):
            if "items" not in intermediate:
                intermediate["items"] = []
            if "type" not in intermediate or intermediate["type"] == "select":
                intermediate["items"].append(make_column(token))
            else:
                intermediate["items"].append(make_table(token))
            return

    if "select" == str(token).lower():
        intermediate["type"] = str(token)
        return

    if "join" in str(token).lower() and 'Keyword' in repr(token):
        intermediate["type"] = str(token)
        for thing in token.tokens:
            tokens.insert(0, thing)
        return

    if "Where" in repr(token):
        token.tokens.pop(0)
        intermediate['where'] = create_dict(token, {})
        return

    if "Parenthesis" in repr(token):
        if "items" not in intermediate:
            intermediate["items"] = []
        intermediate["items"].append(create_dict(token, {}))
        return

    if "Keyword" in repr(token) and "on" == str(token).lower():
        intermediate['on'] = create_dict(token, {}, False)
        return

    if "Function" in repr(token):
        if "items" not in intermediate:
            intermediate["items"] = []
        intermediate["items"].append(make_literal(token))
        return

    if "from" == str(token).lower():
        intermediate['from'] = create_dict(token, {"type": "table"}, False)
        return

    for infix in INFIX:
        if "Keyword" in repr(token) and infix == str(token).lower():
            intermediate['relation'] = {"type": infix, "items": [parse_relation(token.tokens[0]), parse_relation(token.tokens[1])]}
            return

    if "Comparison" in repr(token):
        if DEBUG:
            print "Comparison:", token
        comparison = whitespace_filter(token.tokens)
        intermediate['relation'] = {"type": comparison[1].to_unicode(), "items": [make_literal(comparison[0]), make_literal(comparison[2])]}
        return

    # TODO something with on

    if "items" not in intermediate:
        intermediate["items"] = []

    intermediate["items"].append(make_literal(token))


sample = """SELECT Count(*),CustomerID,
  PaymentDate,
  Amount,
  (SELECT SUM(Ammount)
    FROM table
    WHERE Amount > 0
      AND PaymentDate <= PaymentDate
      AND CustomerID = CustomerID),
  (SELECT SUM(Ammount)
    FROM table
    WHERE CustomerID = CustomerID
    AND PaymentDate <= PaymentDate)
FROM table1 INNEr JOIN table2 oN x = 3
WHERE x > 4 AND y = 'STRING JOIN SELECT FROM' OR l in ('a', 'b')
"""

import sys
try:
    sample = sys.argv[1]
except:
    pass

stmt = parse_statement(sample)
print json.dumps(create_dict(stmt, {}))

if DEBUG:
    tokens = stmt.tokens
    tokens = whitespace_filter(tokens)
    tokens = punctuation_filter(tokens)
    tokens = preprocess_infix(tokens)
    tokens = preprocess_nesting(tokens)
