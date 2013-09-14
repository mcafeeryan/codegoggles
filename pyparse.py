import sqlparse
import json

TYPES = ["SELECT"]
COLUMNS = ["Wildcard", "Identifier"]
INFIX = ["in", "or", "and", "join"]
NEST_ONE = ["from", "on"]

DEBUG = False

RECURSIVE_OPERATIONS = False

TABLE_DEF = {
    "country": ["name", "population", "capital", "gdp", "country_code"],
    "JOIN": ["select", "JOIN", "ON", "GROUPBY", "COUNT", "SUM", "MIN", "WHERE"],
    "IN": ["UPDATE", "INSERT", "MAX", "FROM", "JOIN"],
    "user": ["first_name", "last_name", "country", "code"],
    "users": ["name", "country", "SSN", "password", "code"],
    "Orders": ["last_name", "Amount", "CustomerID", "PaymentDate", "MiscColumn"],
    "table1": ["Amount", "CustomerID", "PaymentDate", "MiscColumn"],
    "table2": ["Amount", "CustomerID", "PaymentDate", "MiscColumn"],
}

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

    def to_unicode(self):
        return " ".join([thing.to_unicode() for thing in self.tokens])

class IdentifierDuck(object):
    def __init__(self, token):
        self.token = token

    def __getattr__(self, name):
        return getattr(self.token, name)

    def __str__(self):
        return str(self.token)

    def __repr__(self):
        return "Identifier" + repr(self.token)

def preprocess_sql(sql):
    sql = " ".join(sql.split())
    return sql

def parse_statement(sql):
    sql = preprocess_sql(sql)
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

def identifierlist_filter(tokens):
    new_tokens = []
    for token in tokens:
        if "IdentifierList" in repr(token):
            new_tokens += token.tokens
        else:
            new_tokens.append(token)
    return new_tokens

def after_from_keyword_to_identifier(tokens):
    for idx in range(len(tokens)):
        token = tokens[idx]
        if "Keyword" in repr(token) and "from" == str(token).lower() and "Parenthesis" not in repr(tokens[idx + 1]):
            tokens[idx + 1] = IdentifierDuck(tokens[idx + 1])
    return tokens

def create_dict(stmt, intermediate, preprocess=True):
    if DEBUG:
        print stmt
    tokens = stmt.tokens
    tokens = identifierlist_filter(tokens)
    tokens = whitespace_filter(tokens)
    tokens = punctuation_filter(tokens)
    tokens = after_from_keyword_to_identifier(tokens)
    if preprocess:
        tokens = preprocess_infix(tokens)
        tokens = preprocess_nesting(tokens)
    while tokens:
        token = tokens.pop(0)
        parse_token(tokens, token, intermediate)
    return intermediate

def make_column(token):
    return {"type": "column", "name": token.to_unicode()}

def make_table(token):
    name = token.to_unicode()
    try:
        return {"type": "table", "name": name, "columns": TABLE_DEF[name]}
    except:
        return {"type": "column", "name": name}

def make_literal(token):
    return {"type": "literal", "name": token.to_unicode()}

def is_join(token):
    return isinstance(token, TokenDuck)

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
        print "token:", token, repr(token)
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
        if RECURSIVE_OPERATIONS:
            intermediate['where'] = create_dict(token, {})
        else:
            intermediate['where'] = token.to_unicode()
        return

    if "Parenthesis" in repr(token):
        if "items" not in intermediate:
            intermediate["items"] = []
        intermediate["items"].append(create_dict(token, {}))
        return

    if "Keyword" in repr(token) and "on" == str(token).lower():
        if RECURSIVE_OPERATIONS:
            intermediate['on'] = create_dict(token, {}, False)
        else:
            intermediate['on'] = token.to_unicode()
        return

    if "Function" in repr(token):
        if "items" not in intermediate:
            intermediate["items"] = []
        intermediate["items"].append(make_literal(token))
        return

    if "from" == str(token).lower():
        # TODO special case join
        if is_join(token.tokens[0]) or "Parenthesis" in repr(token.tokens[0]):
            intermediate['from'] = create_dict(token, {"type": "table"}, False)
        else:
            intermediate['from'] = make_table(token.tokens[0])
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

    if DEBUG:
        print token, repr(token)
    intermediate["items"].append(make_literal(token))

def recursive_dot_removal_dict(target_dict):
    for key in target_dict:
        if isinstance(target_dict[key], dict):
            target_dict[key] = recursive_dot_removal_dict(target_dict[key])
        elif isinstance(target_dict[key], list):
            target_dict[key] = recursive_dot_removal_list(target_dict[key])
        else:
            if key not in ("where", "on"):
                target_dict[key] = dot_removal(target_dict[key])
    return target_dict



def recursive_dot_removal_list(target_list):
    for key in range(len(target_list)):
        if isinstance(target_list[key], dict):
            target_list[key] = recursive_dot_removal_dict(target_list[key])
        elif isinstance(target_list[key], list):
            target_list[key] = recursive_dot_removal_list(target_list[key])
        else:
            target_list[key] = dot_removal(target_list[key])
    return target_list

def dot_removal(target_string):
    return target_string.split('.')[-1]

def to_json(stmt):
    target_dict = create_dict(stmt, {})
    target_dict = recursive_dot_removal_dict(target_dict)
    target_json = json.dumps(target_dict)
    return target_json

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
FROM table1 INNEr JOIN table2 oN CustomerID = CustomerID
WHERE x > 4 AND y = 'STRING JOIN SELECT FROM' OR l in ('a', 'b')
"""

if 0:
    nested_sample = """SELECT Count(*){}
    FROM table1 INNEr JOIN table2 oN x = 3
    WHERE x > 4 AND y = 'STRING JOIN SELECT FROM' OR l in ('a', 'b')
    """

    sample = nested_sample
    for _ in range(10):
        sample = sample.format(",(" + nested_sample + ")")
    sample.format("")

sample = """SELECT user.first_name, user.last_name, country.name
FROM (SELECT user.first_name, user.last_name, country.name
FROM users
JOIN country ON counter_code = code
JOIN country ON country_code = code
WHERE country.name IN ('Kazakhstan', 'Burundi')) JOIN (SELECT Amount FROM table INNER JOIN user on last_name) WHERE Amount < 200"""

import sys
try:
    sample = sys.argv[1]
    DEBUG = False
except:
    DEBUG = True

stmt = parse_statement(sample)
print to_json(stmt)

if DEBUG:
    tokens = stmt.tokens
    tokens = identifierlist_filter(tokens)
    tokens = whitespace_filter(tokens)
    tokens = punctuation_filter(tokens)
    tokens = preprocess_infix(tokens)
    tokens = preprocess_nesting(tokens)
