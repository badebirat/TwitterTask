import re

pattern = '(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9_]+)'

txt = "@always_nidhi @YouTube no i dnt understand bt i loved  Serasi ade haha @AdeRais \"@SMTOWNGLOBAL: #SHINee ONEW(@skehehdanfdldi) and #AMBER(@llama_ajol)"
x = re.findall(pattern, txt)

print(x)
