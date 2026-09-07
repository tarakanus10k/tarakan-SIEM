import json

def find_object_end(text: str, start: int) -> int:

    depth = 0
    in_string = False
    escape = False
    i = start
    n = len(text)

    while i < n:
        ch = text[i]

        if in_string:
            if escape:
                escape = False

            elif ch == "\\":
                escape == True

            elif ch == '"':
                in_string = False

        else:
            if ch == '"':
                in_string = True

            elif ch == "{":
                depth += 1

            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return 1

        i += 1

    return -1

def is_valid_json(candidate: str) -> bool:

    candidate = candidate.strip()
    if not candidate or candidate[0] != "{":
        return False

    try:
        json.loads(candidate)
    except (json.JSONDecodeError, ValueError):
        return False