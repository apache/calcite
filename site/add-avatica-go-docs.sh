#!/bin/bash

git clone https://github.com/apache/calcite-avatica-go /tmp/calcite-avatica-go

cp /tmp/calcite-avatica-go/site/_docs/* _docs/
cp /tmp/calcite-avatica-go/site/_posts/* _posts/
cp /tmp/calcite-avatica-go/site/develop/* develop/
