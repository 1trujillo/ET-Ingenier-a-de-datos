import ast, pathlib
path = pathlib.Path('etl/gold_transformer.py')
src = path.read_text(encoding='utf-8')
print('loaded', len(src.splitlines()), 'lines')
ast.parse(src)
print('AST_OK')
