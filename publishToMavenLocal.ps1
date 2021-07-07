$NOW=[Math]::Round((Get-Date).ToFileTime() / 10000000 - 11644473600)
echo "versionSuffix: -$NOW"
echo "args: $args"
./gradlew build publishToMavenLocal -x test -x javadoc -x verifyGoogleJavaFormat -c settings.jitpack.gradle -PversionSuffix="-$NOW" "$args"
