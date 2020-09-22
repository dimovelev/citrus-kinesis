# Overview
Send and receive messages to / from an AWS Kinesis stream in your integration tests (using e.g. localstack for AWS) based on
[AWS SDK v2](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html).

You might need to disable CBOR for usage with localstack. Programmatically, you can do it like that:

```java
System.setProperty(CBOR_ENABLED.property(), "false");
```
   
# Usage
Check out the test `KinesisTest` and the configuration in `CitrusConfiguration`.
 
# Releasing
* Release the project using `mvn release:prepare`, `mvn release:perform`
* Switch to the released tag using git checkout v[version]
* Make sure that you have your sonatype credentials in your maven settings as server id `sonatype-nexus-releases` otherwise you will get HTTP 401 when trying to upload
* Depending on your local setup, you might need to help GPG know how to ask you for the passphrase - run this export GPG_TTY=$(tty)
* Build and deploy the artifacts to sonatype `mvn clean deploy -Dgpg.keyname="<name>" -Dmaven.test.skip=true -P publish`
* Switch back to master using git checkout master
* Close and release the repository at oss.sonatype.org
* Push the changes to github. Also push the tags (git push --tags).
