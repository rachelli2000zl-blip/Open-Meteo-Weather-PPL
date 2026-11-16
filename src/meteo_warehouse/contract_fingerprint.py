"""Stable Gold syntax fingerprints and a narrow bridge for existing contracts."""

import ast
import hashlib
import json

# The original implementation used a hash of the complete gold.py source file.
# Accept that known hash only for the reviewed, equivalent implementation below.
LEGACY_SOURCE_SHA256 = "d267c351c9c1517fcda43d8106941250707599f3d666e3a041cc61b4c57092a1"
LEGACY_COMPATIBLE_SHA256 = "3105aa6532070b2faca41c130077f8818ca1560c54289a9864c86efc0c15da19"


def implementation_fingerprint(source):
    tree = ast.parse(source)
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)}

    def encode(value):
        if isinstance(value, ast.ImportFrom) and value.module == "datetime" and not value.level:
            # Normalize only the two reviewed unused imports, not arbitrary imports
            # that may register plugins or have other module side effects.
            names = [
                alias
                for alias in value.names
                if alias.name not in {"UTC", "datetime"} or (alias.asname or alias.name) in used
            ]
            if names:
                value.names = names
        if isinstance(value, ast.JoinedStr) and all(
            isinstance(part, ast.Constant) and isinstance(part.value, str) for part in value.values
        ):
            value = ast.Constant(value="".join(part.value for part in value.values))
        if isinstance(value, ast.AST):
            # Ignore locations and absent optional fields, including empty fields
            # added by newer Python versions. Preserve SQL literal contents exactly.
            return {
                "node": type(value).__name__,
                **{name: encode(item) for name, item in ast.iter_fields(value) if item is not None and item != []},
            }
        if isinstance(value, list):
            return [encode(item) for item in value]
        return value

    payload = json.dumps({"format": "gold-ast-v1", "tree": encode(tree)}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fingerprints_match(stored, current):
    return stored == current or (stored == LEGACY_SOURCE_SHA256 and current == LEGACY_COMPATIBLE_SHA256)
