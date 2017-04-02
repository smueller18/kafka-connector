#!/usr/bin/env bash

VERSION=$(python3 -c "import kafka_conector; print(kafka_conector.__version__)")

if [ -f MANIFEST ]; then
    echo "removing manifest file"
    rm MANIFEST
fi

echo "uploading package."
python3 setup.py sdist
twine upload dist/*

echo "creating tag $VERSION"
git tag $VERSION

echo "pushing tag"
git push ${REMOTE:-origin} $VERSION