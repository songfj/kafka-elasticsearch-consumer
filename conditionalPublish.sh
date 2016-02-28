#!/bin/sh

if [ "${TRAVIS_PULL_REQUEST}" = "false" ]; then
    echo '$TRAVIS_PULL_REQUEST is false, skipping tests and publishing'
    ./gradlew clean bintrayUpload -Pbuild=${TRAVIS_BUILD_NUMBER} -Prelease=1.0
else
    echo '$TRAVIS_PULL_REQUEST is not false ($TRAVIS_PULL_REQUEST), running tests'
    ./gradlew clean test
fi