
{s=$0}
/result = self.client.execute\(r'''/ {
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
/result = self.client.execute\("/ {
  mode = 2;
  s = substr(s, length("result = self.client.execute('") + 1)
}
/result = self.execute_query\("/ {
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
/result = self.client.execute\('/ {
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
