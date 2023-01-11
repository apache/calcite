#!/bin/gawk

function header(s, c, i, h) {
    c = 0;
    for (i = 1; i < length(s); i++) {
        if (substr(s, i, 2) == "\\t") {
            ++c;
        }
    }
    h = ""
    for (i = 0; i < c + 1; i++) {
        if (i > 0) {
            h = h ", "
        }
        h = h "EXPR$"
        h = h ("" + i)
    }
    return h
}

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
mode == 0 && s ~ /assert result.data == \[".*"\]/ {
    gsub(/assert result[.]data == \["/, "", s)
    gsub(/"\]/, "", s)
    print header(s)
    gsub(/\\t/, ", ", s)
    print s
    print "!ok"
    next
}
mode == 0 && s ~ /assert result.data == \['.*'\]/ {
    gsub(/assert result[.]data == \['/, "", s)
    gsub(/'\]/, "", s)
    print header(s)
    gsub(/\\t/, ", ", s)
    print s
    print "!ok"
    next
}
mode == 0 && s ~ /assert ".*" in str\(err\)/ {
    gsub(/assert "/, "", s)
    gsub(/" in str\(err\)/, "", s)
    print s
    print "!error"
    next
}
{ print s}
