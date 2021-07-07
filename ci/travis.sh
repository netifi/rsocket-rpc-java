#!/usr/bin/env bash

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then

    echo -e "Building PR #$TRAVIS_PULL_REQUEST [$TRAVIS_PULL_REQUEST_SLUG/$TRAVIS_PULL_REQUEST_BRANCH => $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH]"
    ./gradlew \
        -PversionSuffix="-SNAPSHOT" \
        -PvcProtobufLibs="/c/Program Files/protobuf/lib" \
        -PvcProtobufInclude="/c/Program Files/protobuf/include" \
        build

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$bintrayUser" != "" ] && [ "$TRAVIS_BRANCH" == "master" ] ; then

    echo -e "Building Master Snapshot $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH/$TRAVIS_BUILD_NUMBER"
    ./gradlew \
        -PversionSuffix="-SNAPSHOT" \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PvcProtobufLibs="/c/Program Files/protobuf/lib" -PvcProtobufInclude="/c/Program Files/protobuf/include" \
        -PbuildInfo.build.number="$TRAVIS_BUILD_NUMBER" \
        -PbuildInfo.build.timestamp="$(git show -s --format=%ct HEAD)000" \
        build artifactoryPublish --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$bintrayUser" != "" ] ; then

    echo -e "Building Branch Snapshot $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH/$TRAVIS_BUILD_NUMBER/$TRAVIS_BUILD_NUMBER"
    ./gradlew \
        -PversionSuffix="-${TRAVIS_BRANCH//\//-}-SNAPSHOT" \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PvcProtobufLibs="/c/Program Files/protobuf/lib" -PvcProtobufInclude="/c/Program Files/protobuf/include" \
        -PbuildInfo.build.number="$TRAVIS_BUILD_NUMBER" \
        -PbuildInfo.build.timestamp="$(git show -s --format=%ct HEAD)000" \
        build artifactoryPublish --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ] && [ "$bintrayUser" != "" ] ; then

    echo -e "Building Tag $TRAVIS_REPO_SLUG/$TRAVIS_TAG"
    ./gradlew \
        -Pversion="$TRAVIS_TAG" -Pstage="$TRAVIS_BUILD_STAGE_NAME" \
        -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" \
        -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" \
        -PvcProtobufLibs="/c/Program Files/protobuf/lib" -PvcProtobufInclude="/c/Program Files/protobuf/include" \
        -PbuildInfo.build.number="$TRAVIS_BUILD_NUMBER" \
        -PbuildInfo.build.timestamp="$(git show -s --format=%ct HEAD)000" \
        build bintrayUpload --stacktrace

else

    echo -e "Building $TRAVIS_REPO_SLUG/$TRAVIS_BRANCH"
    ./gradlew \
        -PversionSuffix="-SNAPSHOT" \
        -PvcProtobufLibs="/c/Program Files/protobuf/lib" -PvcProtobufInclude="/c/Program Files/protobuf/include" \
        build

fi

