#!/usr/bin/env bash

# Publish the website.

set -x
set -e

#git pull -r
./gradlew clean check asciidoctor install
git checkout gh-pages

for proj in sdsai-dsds-mongo sdsai-dsds-core sdsai-dsds-riak sdsai-dsds-s3
do
	rm -fr "javadocs/$proj" || true
	mv "$proj/build/docs/javadoc" "javadocs/$proj" || true

	rm -fr "docs/$proj" || true
	mv "$proj/build/asciidoc/html5" "docs/$proj" || true
done

echo Page updated. Make final edits.
