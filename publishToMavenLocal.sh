NOW=$(date +%s)
echo "versionSuffix: -$NOW"
./gradlew build publishToMavenLocal -x test -x javadoc -x verifyGoogleJavaFormat -c settings.jitpack.gradle -PversionSuffix="-$NOW" "$@"
