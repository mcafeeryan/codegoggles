import sqlparse
import json

TYPES = ["SELECT"]
COLUMNS = ["Wildcard", "Identifier"]
INFIX = ["in", "or", "and", "join"]

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

def get_preview(sql):
    # TODO
    pass

def whitespace_filter(tokens):
    return [x for x in tokens if not x.is_whitespace()]

def punctuation_filter(tokens):
    return [x for x in tokens if not "Punctuation" in repr(x)]

def preprocess_nesting(tokens):
    for idx in reversed(range(len(tokens))):
        token = tokens[idx]
        if isinstance(token, TokenDuck):
            continue
        for infix in INFIX:
            if "Keyword" in repr(tokens[idx]) and (infix == str(tokens[idx]).lower() or (infix == "join" and infix in str(tokens[idx]).lower())):
                tokens[idx] = TokenDuck(token, [tokens.pop(idx + 1), tokens.pop(idx + 1)])
        if "Keyword" in repr(token) and "from" in str(token).lower():
            tokens[idx] = TokenDuck(token, [tokens.pop(idx + 1)])
    return tokens


def create_dict(stmt, intermediate, preprocess=True):
    tokens = stmt.tokens
    if preprocess:
        tokens = whitespace_filter(tokens)
        tokens = punctuation_filter(tokens)
        tokens = preprocess_infix(tokens)
        tokens = preprocess_nesting(tokens)
    while tokens:
        token = tokens.pop(0)
        parse_token(tokens, token, intermediate)
    # intermediate['preview'] = get_preview(sql)
    return intermediate

def make_column(token):
    return {"type": "column", "name": token.to_unicode()}

def make_table(token):
    return {"type": "table", "name": token.to_unicode()}

def make_literal(token):
    return {"type": "table", "name": token.to_unicode()}

def preprocess_infix(tokens):
    for infix in INFIX:
        for idx in range(1, len(tokens) - 1):
            if "Keyword" in repr(tokens[idx]) and (infix == str(tokens[idx]).lower() or (infix == "join" and infix in str(tokens[idx]).lower())):
                tokens[idx - 1], tokens[idx] = tokens[idx], tokens[idx - 1]
    return tokens

def parse_token(tokens, token, intermediate):
    for column in COLUMNS:
        if column in repr(token):
            if "items" not in intermediate:
                intermediate["items"] = []
            if intermediate["type"] == "select":
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
        return  # TODO

    if "Function" in repr(token):
        return # TODO

    if "from" == str(token).lower():
        intermediate['from'] = create_dict(token, {"type": "table"}, False)
        return

    for infix in INFIX:
        if "Keyword" in repr(token) and infix == str(token).lower():
            intermediate['relation'] = create_dict(token, {"type": infix}, False)
            return

    if "Comparison" in repr(token):
        comparison = whitespace_filter(token.tokens)
        intermediate['relation'] = {"type": comparison[1].to_unicode(), "items": [make_literal(comparison[0]), make_literal(comparison[2])]}
        return


    if "items" not in intermediate:
        intermediate["items"] = []

    intermediate["items"].append(make_literal(token))


sample = """SELECT Count(*),CustomerID,
  PaymentDate,
  Amount,
  (SELECT SUM(Ammount)
    FROM table as ALIAS
    WHERE ALIAS.Amount > 0
      AND ALIAS.PaymentDate <= PaymentDate
      AND ALIAS.CustomerID = CustomerID),
  (SELECT SUM(Ammount)
    FROM table as ALIAS
    WHERE ALIAS.CustomerID = CustomerID
    AND ALIAS.PaymentDate <= PaymentDate)
FROM table1 INNEr JOIN table2
WHERE x > '4.5' AND y = 'STRING JOIN SELECT FROM' OR l in ('a', 'b')
"""

stmt = parse_statement(sample)
print json.dumps(create_dict(stmt, {}))
tokens = stmt.tokens
tokens = whitespace_filter(tokens)
tokens = punctuation_filter(tokens)
tokens = preprocess_infix(tokens)
tokens = preprocess_nesting(tokens)
