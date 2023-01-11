
{s=$0}
/err = self.execute_query_expect_failure\(self.client, / {
  gsub(/err = self.execute_query_expect_failure\(self.client, /, "result = self.client.execute(", s)
}
s ~ /result = self.client.execute\(r'''/ {
  mode=1;
  s = substr(s, length("result = self.client.execute(r'''") + 1)
}
mode == 1 {
  gsub(/ *'''$/, "", s)
}
mode == 1 {
  gsub(/^ *r'''/, "    ", s)
}
mode == 1 && /'''\)$/ {
  gsub(/'''\)$/, ";", s); mode = 0;
}
s ~ /result = self.client.execute\("/ {
  mode = 2;
  s = substr(s, length("result = self.client.execute('") + 1)
}
s ~ /result = self.execute_query\("/ {
  mode = 2;
  s = substr(s, length("result = self.execute_query('") + 1)
}
mode == 2 {
  gsub(/ *"$/, "", s)
}
mode == 2 {
  gsub(/^ *"/, "    ", s)
}
mode == 2 && /"\)$/ {
  gsub(/"\)$/, ";", s); mode = 0;
}
s ~ /result = self.client.execute\('/ {
    mode = 3;
    s = substr(s, length("result = self.client.execute('") + 1)
}
mode == 3 {
    gsub(/ *'$/, "", s)
}
mode == 3 {
    gsub(/^ *'/, "    ", s)
}
mode == 3 && /'\)$/ {
    gsub(/'\)$/, ";", s); mode = 0;
}
{ print s}
